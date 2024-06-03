#!/usr/bin/python3
import argparse
from boto3 import session
import json
import os
import sqlalchemy
import subprocess

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import lit, col

s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']

variants_schema = StructType(
    [
        StructField('varId', StringType(), nullable=False),
        StructField('chromosome', StringType(), nullable=False),
        StructField('position', IntegerType(), nullable=False),
        StructField('reference', StringType(), nullable=False),
        StructField('alt', StringType(), nullable=False),
        StructField('phenotype', StringType(), nullable=False),
        StructField('pValue', DoubleType(), nullable=False),
        StructField('beta', DoubleType(), nullable=False),
        StructField('stdErr', DoubleType(), nullable=False),
        StructField('n', DoubleType(), nullable=False),
    ]
)


class BioIndexDB:
    def __init__(self):
        self.secret_id = 'dig-bio-portal'
        self.region = 'us-east-1'
        self.config = None
        self.engine = None

    def get_config(self):
        if self.config is None:
            client = session.Session(region_name=self.region).client('secretsmanager')
            self.config = json.loads(client.get_secret_value(SecretId=self.secret_id)['SecretString'])
        return self.config

    def get_engine(self):
        if self.engine is None:
            self.config = self.get_config()
            print(f'creating engine for {self.config["host"]}:{self.config["port"]}/{self.config["dbname"]}')
            self.engine = sqlalchemy.create_engine('{engine}://{username}:{password}@{host}:{port}/{db}'.format(
                engine=self.config['engine'] + ('+pymysql' if self.config['engine'] == 'mysql' else ''),
                username=self.config['username'],
                password=self.config['password'],
                host=self.config['host'],
                port=self.config['port'],
                db=self.config['dbname']
            ))
        return self.engine

    def get_sorted_datasets(self, phenotype, ancestry):
        with self.get_engine().connect() as connection:
            print(f'Querying db for phenotype {phenotype} for largest {ancestry} dataset')
            ancestry_addendum = f'AND ancestry="{ancestry}" ' if ancestry != 'TE' else ''
            query = sqlalchemy.text(
                f'SELECT name, ancestry FROM Datasets '
                f'WHERE REGEXP_LIKE(phenotypes, "(^|,){phenotype}($|,)") '
                f'{ancestry_addendum}AND tech="GWAS" '
                f'ORDER BY subjects DESC'
            )
            rows = connection.execute(query).all()
        print(f'Returned {len(rows)} rows for largest dataset')
        return [(row[0], row[1]) for row in rows]


def check_existence(phenotype, dataset_ancestry):
    path = f'{s3_in}/out/metaanalysis/variants/{phenotype}/dataset={dataset_ancestry[0]}/ancestry={dataset_ancestry[1]}/'
    return subprocess.call(['aws', 's3', 'ls', path, '--recursive'])


def get_dataset_ancestry(phenotype, ancestry):
    db = BioIndexDB()
    dataset_ancestries = db.get_sorted_datasets(phenotype, ancestry)
    for dataset_ancestry in dataset_ancestries:
        if not check_existence(phenotype, dataset_ancestry):
            return dataset_ancestry


def main():
    opts = argparse.ArgumentParser()
    opts.add_argument('phenotype')
    opts.add_argument('ancestry')

    # parse the command line parameters
    args = opts.parse_args()

    # create a spark session
    spark = SparkSession.builder.appName('bottom-line').getOrCreate()

    # get the source and output directories
    dataset_ancestry = get_dataset_ancestry(args.phenotype, args.ancestry)
    print(f'Largest GWAS dataset for phenotype {args.phenotype}, ancestry {args.ancestry}: {dataset_ancestry}')
    if dataset_ancestry is not None:
        dataset, ancestry = dataset_ancestry
        srcdir = f'{s3_in}/out/metaanalysis/variants/{args.phenotype}/dataset={dataset}/ancestry={ancestry}/*/part-*'
        if args.ancestry == 'TE':
            outdir = f'{s3_out}/out/metaanalysis/largest/trans-ethnic/{args.phenotype}/'
        else:
            outdir = f'{s3_out}/out/metaanalysis/largest/ancestry-specific/{args.phenotype}/ancestry={args.ancestry}/'

        columns = [col(field.name) for field in variants_schema]

        output_ancestry = args.ancestry if args.ancestry != 'TE' else 'Mixed'

        df = spark.read \
            .csv(
            srcdir,
            sep='\t',
            header=True,
            schema=variants_schema,
        ) \
            .select(*columns) \
            .withColumn('ancestry', lit(output_ancestry))

        df.write \
            .mode('overwrite') \
            .option("compression", "org.apache.hadoop.io.compress.ZStandardCodec") \
            .json(outdir)
    spark.stop()


if __name__ == '__main__':
    main()
