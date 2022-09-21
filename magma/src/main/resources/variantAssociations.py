import argparse
from boto3 import session
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
import sqlalchemy

s3dir = f's3://dig-analysis-data'


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

    def get_largest_mixed_dataset(self, phenotype):
        with self.get_engine().connect() as connection:
            print(f'Querying db for phenotype {phenotype} for largest mixed dataset')
            rows = connection.execute(f'SELECT tech, name FROM Datasets '
                                      f'WHERE REGEXP_LIKE(phenotypes, "(^|,){phenotype}($|,)") '
                                      f'AND ancestry="Mixed" AND tech="GWAS" '
                                      f'ORDER BY subjects DESC LIMIT 1').all()
        print(f'Returned {len(rows)} rows for largest mixed dataset')
        if len(rows) == 1:
            return f'{rows[0][0]}/{rows[0][1]}'


def get_s3_dir(phenotype, ancestry):
    if ancestry == 'Mixed':
        db = BioIndexDB()
        tech_dataset = db.get_largest_mixed_dataset(phenotype)
        if tech_dataset is not None:
            return f'{s3dir}/variants/{tech_dataset}/{phenotype}/'
    else:
        return f'{s3dir}/out/metaanalysis/ancestry-specific/{phenotype}/ancestry={ancestry}/'


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

    srcdir = get_s3_dir(args.phenotype, args.ancestry)
    if srcdir is not None:
        print(f'Using directory: {srcdir}')

        # other input and output directories
        snpdir = f'{s3dir}/out/varianteffect/snp'
        outdir = f'{s3dir}/out/magma/variant-associations/{args.phenotype}/ancestry={args.ancestry}'

        # start spark
        spark = SparkSession.builder.appName('magma').getOrCreate()

        # load variants and phenotype associations
        df = spark.read.json(f'{srcdir}/part-*')
        snps = spark.read.csv(f'{snpdir}/part-*', sep='\t', header=True)

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
