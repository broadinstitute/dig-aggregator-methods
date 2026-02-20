#!/usr/bin/python3
import argparse
from boto3 import session
import json
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
import sqlalchemy
import subprocess

s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']


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

    def get_sorted_datasets(self, phenotype):
        with self.get_engine().connect() as connection:
            print(f'Querying db for phenotype {phenotype} for largest {ancestry} dataset')
            query = sqlalchemy.text(
                f'SELECT name FROM Datasets '
                f'WHERE REGEXP_LIKE(phenotypes, "(^|,){phenotype}($|,)") '
                f'AND ancestry="Mixed" AND tech="GWAS" '
                f'ORDER BY subjects DESC'
            )
            rows = connection.execute(query).all()
        print(f'Returned {len(rows)} rows for largest dataset')
        return [row[0] for row in rows]


def check_existence(phenotype, dataset):
    path = f'{s3_in}/out/metaanalysis/variants/{phenotype}/dataset={dataset}/ancestry=Mixed/'
    return subprocess.call(['aws', 's3', 'ls', path, '--recursive'])


def get_s3_dir(phenotype, ancestry):
    if ancestry == 'Mixed':
        db = BioIndexDB()
        datasets = db.get_sorted_datasets(phenotype)
        for dataset in datasets:
            if not check_existence(phenotype, dataset):
                return f'{s3_in}/variants/GWAS/{dataset}/{phenotype}/'
    else:
        return f'{s3_in}/out/metaanalysis/bottom-line/ancestry-specific/{phenotype}/ancestry={ancestry}/'


def main():
    """
     Arguments:  phenotype
                 ancestry
     """
    opts = argparse.ArgumentParser()
    opts.add_argument('--phenotype', type=str, required=True)
    opts.add_argument('--ancestry', type=str, required=True)

    # parse command line
    args = opts.parse_args()

    # start spark
    spark = SparkSession.builder.appName('magma').getOrCreate()

    # input and output directories
    outdir = f'{s3_out}/out/magma/variant-associations/{args.phenotype}/ancestry={args.ancestry}'
    srcdir = get_s3_dir(args.phenotype, args.ancestry)
    if srcdir is not None:
        print(f'Using directory: {srcdir}')

        # load variants and phenotype associations
        df = spark.read.json(f'{srcdir}/part-*')
        snps = spark.read.csv(f's3://dig-analysis-bin/snps/dbSNP_common_GRCh37.csv', sep='\t', header=True)

        # drop variants with no dbSNP and join
        snps = snps.filter(snps.dbSNP.isNotNull())

        # join to get the rsID for each
        df = df.join(snps, on='varId')

        # keep only the columns magma needs in the correct order
        df = df.select(
            df.dbSNP.alias('SNP'),
            df.pValue.alias('P'),
            df.n.cast(IntegerType()).alias('subjects'),
        )

        # output results
        df.write \
            .mode('overwrite') \
            .csv(outdir, sep='\t', header='true')

    # done
    spark.stop()


if __name__ == '__main__':
    main()
