#!/usr/bin/python3
import argparse
from boto3 import session
import json
import re
import sqlalchemy
import subprocess

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import lit, col

s3dir = 's3://dig-analysis-data'

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

    def get_largest_dataset(self, phenotype, ancestry):
        with self.get_engine().connect() as connection:
            print(f'Querying db for phenotype {phenotype} for largest {ancestry} dataset')
            query = sqlalchemy.text(
                f'SELECT name FROM Datasets '
                f'WHERE REGEXP_LIKE(phenotypes, "(^|,){phenotype}($|,)") '
                f'AND ancestry="{ancestry}" AND tech="GWAS" '
                f'ORDER BY subjects DESC LIMIT 1'
            )
            rows = connection.execute(query).all()
        print(f'Returned {len(rows)} rows for largest mixed dataset')
        if len(rows) == 1:
            return rows[0][0]


def get_dataset(phenotype, ancestry):
    db = BioIndexDB()
    return db.get_largest_dataset(phenotype, ancestry)


def main():
    opts = argparse.ArgumentParser()
    opts.add_argument('phenotype')
    opts.add_argument('ancestry')

    # parse the command line parameters
    args = opts.parse_args()

    # create a spark session
    spark = SparkSession.builder.appName('bottom-line').getOrCreate()

    # get the source and output directories
    dataset = get_dataset(args.phenotype, args.ancestry)
    print(f'Largest GWAS dataset for phenotype {args.phenotype}, ancestry {args.ancestry}: {dataset}')
    if dataset is not None:
        srcdir = f'{s3dir}/out/metaanalysis/variants/{args.phenotype}/dataset={dataset}/ancestry={args.ancestry}/*/part-*'
        outdir = f'{s3dir}/out/metaanalysis/largest/ancestry-specific/{args.phenotype}/ancestry={args.ancestry}/'

        columns = [col(field.name) for field in variants_schema]

        df = spark.read \
            .csv(
            srcdir,
            sep='\t',
            header=True,
            schema=variants_schema,
        ) \
            .select(*columns) \
            .withColumn('ancestry', lit(args.ancestry))

        df.write \
            .mode('overwrite') \
            .option("compression", "org.apache.hadoop.io.compress.ZStandardCodec") \
            .json(outdir)
    spark.stop()


if __name__ == '__main__':
    main()
