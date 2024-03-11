#!/usr/bin/python3
import argparse
from boto3.session import Session
import math
import json
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import mean
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
import sqlalchemy
import subprocess

MAF_SCALING_THRESHOLD = 2
FALLBACK_SCALING_THRESHOLD = 5
TRAINING_DATA_MINIMUM_COUNT = 1000

s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']


class PortalDB:
    def __init__(self):
        self.secret_id = os.environ['PORTAL_SECRET']
        self.db_name = os.environ['PORTAL_DB']
        self.config = None
        self.engine = None

    def get_config(self):
        if self.config is None:
            client = Session().client('secretsmanager')
            self.config = json.loads(client.get_secret_value(SecretId=self.secret_id)['SecretString'])
        return self.config

    def get_engine(self):
        if self.engine is None:
            self.config = self.get_config()
            self.engine = sqlalchemy.create_engine('{engine}://{username}:{password}@{host}:{port}/{db}'.format(
                engine=self.config['engine'] + ('+pymysql' if self.config['engine'] == 'mysql' else ''),
                username=self.config['username'],
                password=self.config['password'],
                host=self.config['host'],
                port=self.config['port'],
                db=self.db_name
            ))
        return self.engine

    def get_is_dichotomous(self, phenotype_name):
        with self.get_engine().connect() as connection:
            query = sqlalchemy.text(f'SELECT name, dichotomous FROM Phenotypes WHERE name = \"{phenotype_name}\"')
            rows = connection.execute(query).all()
            if len(rows) != 1:
                raise Exception(f"Invalid number of rows returned ({len(rows)}) for phenotype {phenotype_name}."
                                f"Check the database and try again.")
            if rows[0][1] is None:
                raise Exception(f"Invalid dichotomous information ({rows[0][1]}) for phenotype {phenotype_name}."
                                f"Check the database and try again.")
            return rows[0][1] == 1

    def get_dataset_data(self, dataset):
        with self.get_engine().connect() as connection:
            rows = connection.execute(
                sqlalchemy.text(f'SELECT name, ancestry FROM Datasets WHERE name = \"{dataset}\"')
            ).all()
        if len(rows) != 1:
            raise Exception(f"Impossible number of rows returned ({len(rows)}) for phenotype {dataset}. "
                            f"Check the database and try again.")
        if rows[0][0] is None or rows[0][1] is None:
            raise Exception(f"Invalid name / ancestry information ({rows[0][0]} / {rows[0][1]}) for dataset {dataset}. "
                            f"Check the database and try again.")
        return {'name': rows[0][0], 'ancestry': rows[0][1]}


class ScalingLogger:
    def __init__(self, log_file):
        self.log_file = log_file
        self.logs = []

    def log(self, s):
        print(s)
        self.logs.append(s)

    def save(self):
        with open(self.log_file, 'w') as f:
            for log in self.logs:
                f.write(f'{log}\n')


def get_fallback_scaling_factor(df, logger):
    stdErr_upper = df.approxQuantile('stdErr', [0.75], 0.001)[0]
    stdErr_lower = df.approxQuantile('stdErr', [0.25], 0.001)[0]
    logger.log(f'Fallback upper and lower 25% quartile stdErr: {stdErr_lower}, {stdErr_upper}')
    scaling_factor = df \
        .filter((df.stdErr < stdErr_upper) & (df.stdErr > stdErr_lower)) \
        .withColumn('stdErr_squared_n',  df.stdErr * (df.n ** 0.5) * math.sqrt(0.25 * 0.75)) \
        .select(mean('stdErr_squared_n')) \
        .collect()[0][0]
    logger.log(f'Estimate of fallback scaling factor: {scaling_factor}')
    return scaling_factor


def get_training_data(df, logger):
    filtered_df = df \
        .filter((df.maf.isNotNull()) & (df.maf >= 0.05))
    logger.log(f'Datapoints with MAF available: {filtered_df.count()}')
    if filtered_df.count() > 0:
        stdErr_upper = filtered_df.approxQuantile('stdErr', [0.75], 0.001)[0]
        stdErr_lower = filtered_df.approxQuantile('stdErr', [0.25], 0.001)[0]
        logger.log(f'Upper and lower 25% quartile stdErr: {stdErr_lower}, {stdErr_upper}')
        filtered_df = filtered_df \
            .filter((filtered_df.stdErr < stdErr_upper) & (filtered_df.stdErr > stdErr_lower))

    training = filtered_df.withColumn('inverse_var_N', 1.0 / (2.0 * df.maf * (1 - df.maf) * df.n)) \
        .withColumn('stdErr_squared', df.stdErr * df.stdErr) \
        .select('stdErr_squared', 'inverse_var_N')

    assembler = VectorAssembler(
        inputCols=['inverse_var_N'],
        outputCol="features")
    return assembler.transform(training).select(['stdErr_squared', 'inverse_var_N', 'features'])


def get_regression_scaling_factor(training, logger):
    if training.count() > TRAINING_DATA_MINIMUM_COUNT:
        lr = LinearRegression(featuresCol='features', labelCol='stdErr_squared') \
            .setFitIntercept(False)
        lr_model = lr.fit(training)

        scaling_factor = math.sqrt(lr_model.coefficients.values[0])
        logger.log(f'Calculated scaling factor is {scaling_factor}')
        return scaling_factor
    logger.log('Not enough datapoints to estimate scaling factor')


def get_scaling_factor(df, tech, logger):
    training = get_training_data(df, logger)
    training_count = training.count()
    logger.log(f'{training_count} datapoints in training dataset')
    regression_scaling_factor = get_regression_scaling_factor(training, logger)
    fallback_scaling_factor = get_fallback_scaling_factor(df, logger)
    if regression_scaling_factor is not None:
        logger.log(f'Using threshold {MAF_SCALING_THRESHOLD}')
        if not (1 / MAF_SCALING_THRESHOLD < regression_scaling_factor < MAF_SCALING_THRESHOLD):
            return regression_scaling_factor
        else:
            return 1.0
    elif tech == 'GWAS':
        logger.log(f'Using threshold {FALLBACK_SCALING_THRESHOLD}')
        if not (1 / FALLBACK_SCALING_THRESHOLD < fallback_scaling_factor < FALLBACK_SCALING_THRESHOLD):
            return fallback_scaling_factor
        else:
            return 1.0
    else:
        logger.log(f'Inspection necessary')
        return 1.0


# NOTE: DF write must go before log upload, otherwise the log will be deleted
def write_to_s3(outdir, logger, df):
    df.write \
        .mode('overwrite') \
        .option("ignoreNullFields", "false") \
        .option("compression", "org.apache.hadoop.io.compress.ZStandardCodec") \
        .json(outdir)

    logger.save()
    subprocess.check_call(['aws', 's3', 'cp', logger.log_file, f'{outdir}/'])
    os.remove(logger.log_file)


def main():
    opts = argparse.ArgumentParser()
    opts.add_argument('method_dataset_phenotype')
    args = opts.parse_args()

    db = PortalDB()
    tech, dataset, phenotype = args.method_dataset_phenotype.split('/')
    is_dichotomous = db.get_is_dichotomous(phenotype)

    srcdir = f'{s3_in}/variants_qc/{args.method_dataset_phenotype}/pass'
    outdir = f'{s3_out}/variants/{args.method_dataset_phenotype}'

    logger = ScalingLogger('scaling.log')
    logger.log(f'Reading from {srcdir}')
    logger.log(f'Writing to {outdir}')
    logger.log(f'Tech: {tech}')
    logger.log(f'Dataset: {dataset}')
    logger.log(f'Phenotype: {phenotype}')
    logger.log(f'Is Dichotomous: {is_dichotomous}')

    spark = SparkSession.builder.appName('scaling').getOrCreate()

    df = spark.read.json(f'{srcdir}/part-*')

    df = df \
        .withColumn('unscaled_beta', df.beta) \
        .withColumn('unscaled_stdErr', df.stdErr)

    if not is_dichotomous:
        scaling_factor = get_scaling_factor(df, tech, logger)
        logger.log(f'Final scaling factor: {scaling_factor}')
        df = df \
            .withColumn('beta', df.beta / scaling_factor) \
            .withColumn('stdErr', df.stdErr / scaling_factor)
    else:
        logger.log('Dichotomous trait. Leaving beta/stdErr unscaled')
    logger.save()

    write_to_s3(outdir, logger, df)

    spark.stop()


if __name__ == '__main__':
    main()
