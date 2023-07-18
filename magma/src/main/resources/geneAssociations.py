#!/usr/bin/python3
import argparse
from boto3 import session
import json
import sqlalchemy
import subprocess

magma_dir = '/mnt/var/magma'

ancestry_to_g1000 = {
    'AA': 'afr',
    'AF': 'afr',
    'SSAF': 'afr',
    'HS': 'amr',
    'EA': 'eas',
    'EU': 'eur',
    'SA': 'sas',
    'GME': 'sas',
    'Mixed': 'eur'
}


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

    def get_dataset_ancestry(self, dataset):
        with self.get_engine().connect() as connection:
            print(f'Querying db for dataset {dataset} ancestry')
            query = sqlalchemy.text(
                f'SELECT name, ancestry FROM Datasets '
                f'WHERE name = :dataset LIMIT 1'
            )
            rows = connection.execute(query, {'dataset': dataset}).all()
        print(f'Returned {len(rows)} rows for dataset to ancestry')
        if len(rows) == 1:
            return rows[0][1]


def run_gene_associations(phenotype, input_type, input, g1000_ancestry):
    return subprocess.check_call([f'{magma_dir}/geneAssociations.sh', phenotype, input_type, input, g1000_ancestry])


def main():
    opts = argparse.ArgumentParser()
    opts.add_argument('--phenotype', type=str, required=True)
    opts.add_argument('--ancestry', type=str, required=False)
    opts.add_argument('--dataset', type=str, required=False)

    # parse command line
    args = opts.parse_args()

    if args.ancestry is None and args.dataset is None:
        raise Exception('Must define either --ancestry or --dataset flag')
    if args.ancestry is not None and args.dataset is not None:
        raise Exception('Cannot define both --ancestry and --dataset flag')

    if args.ancestry is not None:
        run_gene_associations(args.phenotype, 'ancestry', args.ancestry, ancestry_to_g1000[args.ancestry])
    else:
        db = BioIndexDB()
        maybe_ancestry = db.get_dataset_ancestry(args.dataset)
        if maybe_ancestry is not None:
            run_gene_associations(args.phenotype, 'dataset', args.dataset, ancestry_to_g1000[maybe_ancestry])


if __name__ == '__main__':
    main()
