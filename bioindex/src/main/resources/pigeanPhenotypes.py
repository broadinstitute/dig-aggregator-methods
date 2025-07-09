#!/usr/bin/python3
from boto3.session import Session
import json
import os
import sqlalchemy
import subprocess

s3_bioindex = os.environ['BIOINDEX_PATH']

outdir = f'{s3_bioindex}/pigean/phenotypes/'


class BioIndexDB:
    def __init__(self):
        self.secret_id = os.environ['PORTAL_SECRET']
        self.db_name = os.environ['PORTAL_DB']
        self.region = 'us-east-1'
        self.config = None
        self.engine = None

    def get_config(self):
        if self.config is None:
            client = Session().client('secretsmanager', region_name=self.region)
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

    def get_phenotype_map(self):
        with self.get_engine().connect() as connection:
            print(f'Querying db for phenotype to name dictionary')
            query = sqlalchemy.text(
                f'SELECT name, description FROM Phenotypes'
            )
            rows = connection.execute(query).all()
        print(f'Returned {len(rows)} phenotypes')
        return {row[0]: row[1] for row in rows}


def get_gcat_map():
    subprocess.check_call('aws s3 cp s3://dig-analysis-bin/gwas_catalog/code_to_name.tsv .', shell=True)
    out = {}
    with open('code_to_name.tsv', 'r') as f:
        for line in f:
            name, description = line.strip().split('\t')
            out[name] = description
    os.remove('code_to_name.tsv')
    return out

def get_orphanet_map():
    subprocess.check_call('aws s3 cp s3://dig-analysis-bin/orphanet/code_to_name.tsv .', shell=True)
    out = {}
    with open('code_to_name.tsv', 'r') as f:
        for line in f:
            name, description = line.strip().split('\t')
            out[name] = description
    os.remove('code_to_name.tsv')
    return out


def clean(s):
    return s.replace(',', ';').replace('"', '\'').encode('utf-8').decode('ascii', errors='ignore')


def upload_maps(all_maps):
    with open('part-00000.json', 'w') as f:
        for trait_group, group_map in all_maps.items():
            for name, description in group_map.items():
                f.write(f'{{"trait_group": "{trait_group}", '
                        f'"phenotype": "{name}", '
                        f'"phenotype_name": "{clean(description)}", '
                        f'"dummy": 1}}\n')
    subprocess.check_call(f'aws s3 cp part-00000.json {outdir}', shell=True)

def main():
    db = BioIndexDB()
    all_maps = {
        'portal': db.get_phenotype_map(),
        'gcat_trait': get_gcat_map(),
        'rare_v2': get_orphanet_map()
    }
    upload_maps(all_maps)


if __name__ == '__main__':
    main()
