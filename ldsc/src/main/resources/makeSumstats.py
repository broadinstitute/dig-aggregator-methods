#!/usr/bin/python3
import argparse
from boto3 import session
import glob
import json
import os
import shutil
import sqlalchemy
import subprocess


# This map is used to map portal ancestries to g1000 ancestries
ancestry_map = {
    'AA': 'AFR',
    'AF': 'AFR',
    'SSAF': 'AFR',
    'HS': 'AMR',
    'EA': 'EAS',
    'EU': 'EUR',
    'SA': 'SAS',
    'GME': 'SAS',
    'Mixed': 'EUR'
}

downloaded_files = '/mnt/var/ldsc'
ldsc_files = f'{downloaded_files}/ldsc'
phenotype_files = '.'
snp_file = f'{downloaded_files}/snps'


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
            return f's3://dig-analysis-data/variants/{tech_dataset}/{phenotype}/'
    else:
        return f's3://dig-analysis-data/out/metaanalysis/ancestry-specific/{phenotype}/ancestry={ancestry}/'


def get_single_json_file(s3_dir, phenotype, ancestry):
    subprocess.check_call(['aws', 's3', 'cp', s3_dir, f'{phenotype_files}/', '--recursive', '--exclude=_SUCCESS', '--exclude=metadata'])
    with open(f'{phenotype_files}/{phenotype}_{ancestry}.json', 'w') as f_out:
        for file in glob.glob(f'{phenotype_files}/part-*.json', recursive=True):
            with open(file, 'r') as f_in:
                shutil.copyfileobj(f_in, f_out)
            os.remove(file)


def upload_and_remove_files(phenotype, ancestry):
    s3_dir = f's3://dig-analysis-data/out/ldsc/sumstats/{phenotype}/ancestry={ancestry}/'
    subprocess.check_call(['aws', 's3', 'cp', f'{phenotype_files}/{phenotype}_{ancestry}.log', s3_dir])
    subprocess.check_call(['aws', 's3', 'cp', f'{phenotype_files}/{phenotype}_{ancestry}.sumstats.gz', s3_dir])
    for file in glob.glob(f'{phenotype_files}/{phenotype}_{ancestry}.*'):
        os.remove(file)


def get_snp_map():
    snp_map = {}
    with open(f'{downloaded_files}/snp.csv', 'r') as f:
        header = f.readline()
        for full_row in f.readlines():
            row = full_row.split('\t')
            snp_map[row[1].strip()] = row[0].strip()
    return snp_map


def stream_to_txt(phenotype, ancestry, snp_map):
    with open(f'{phenotype_files}/{phenotype}_{ancestry}.json', 'r') as f_in:
        with open(f'{phenotype_files}/{phenotype}_{ancestry}.txt', 'w') as f_out:
            line_template = '{}\t{}\t{}\t{}\t{}\t{}\n'
            f_out.write(line_template.format('MarkerName', 'Allele1', 'Allele2', 'p', 'beta', 'N'))
            json_string = f_in.readline()
            while len(json_string) > 0:
                for json_substring in json_string.replace('}{', '}\n{').splitlines():
                    line = json.loads(json_substring)
                    if 'varId' in line and line['varId'] in snp_map and line['beta'] is not None:
                        line_string = line_template.format(
                            snp_map[line['varId']],
                            line['reference'].lower(),
                            line['alt'].lower(),
                            line['pValue'],
                            line['beta'],
                            line['n']
                        )
                        f_out.write(line_string)
                json_string = f_in.readline()


def create_sumstats(phenotype, ancestry):
    subprocess.check_call([
        'python3', f'{ldsc_files}/munge_sumstats.py',
        '--sumstats', f'{phenotype_files}/{phenotype}_{ancestry}.txt',
        '--out', f'{phenotype_files}/{phenotype}_{ancestry}',
        '--merge-alleles', f'{snp_file}/w_hm3.snplist'
    ])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--phenotype', default=None, required=True, type=str,
                        help="Input phenotype.")
    parser.add_argument('--ancestry', default=None, required=True, type=str,
                        help="Ancestry, should be two letter version (e.g. EU) and will be made upper.")
    args = parser.parse_args()
    phenotype = args.phenotype
    ancestry = args.ancestry
    if ancestry not in ancestry_map:
        raise Exception(f'Invalid ancestry ({ancestry}), must be one of {", ".join(ancestry_map.keys())}')

    s3_dir = get_s3_dir(phenotype, ancestry)
    if s3_dir is not None:
        print(f'Using directory: {s3_dir}')
        get_single_json_file(s3_dir, phenotype, ancestry)
        snp_map = get_snp_map()
        print(f'Created SNP map ({len(snp_map)} variants)')
        stream_to_txt(phenotype, ancestry, snp_map)
        create_sumstats(phenotype, ancestry)
        upload_and_remove_files(phenotype, ancestry)


if __name__ == '__main__':
    main()
