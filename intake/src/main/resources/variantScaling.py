#!/usr/bin/python3
import argparse
from boto3.session import Session
import json
import math
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
s3dir = 's3://dig-analysis-data'


class BioIndexDB:
    def __init__(self):
        self.secret_id = 'dig-bio-portal'
        self.region = 'us-east-1'
        self.config = None
        self.engine = None

    def get_config(self):
        if self.config is None:
            client = Session().client('secretsmanager', region_name=self.region)
            self.config = json.loads(client.get_secret_value(SecretId='dig-bio-portal')['SecretString'])
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
                db=self.config['dbname']
            ))
        return self.engine

    def get_is_dichotomous(self, phenotype_name):
        with self.get_engine().connect() as connection:
            rows = connection.execute(f'SELECT name, dichotomous FROM Phenotypes WHERE name = \"{phenotype_name}\"').all()
            if len(rows) != 1:
                raise Exception(f"Impossible number of rows returned ({len(rows)}) for phenotype {phenotype_name}."
                                f"Check the database and try again.")
            return rows[0][1] == 1


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

    db = BioIndexDB()
    tech, dataset, phenotype = args.method_dataset_phenotype.split('/')
    is_dichotomous = db.get_is_dichotomous(phenotype)

    srcdir = f'{s3dir}/variants_qc/{args.method_dataset_phenotype}/pass'
    outdir = f'{s3dir}/variants/{args.method_dataset_phenotype}'

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
