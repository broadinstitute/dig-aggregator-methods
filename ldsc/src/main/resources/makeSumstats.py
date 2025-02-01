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

s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']

valid_chromosomes = {str(i) for i in range(1, 23)}


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

    def get_largest_mixed_datasets(self, phenotype):
        with self.get_engine().connect() as connection:
            print(f'Querying db for phenotype {phenotype} for largest mixed dataset')
            query = sqlalchemy.text(f'SELECT tech, name FROM Datasets '
                                    f'WHERE REGEXP_LIKE(phenotypes, "(^|,){phenotype}($|,)") '
                                    f'AND ancestry="Mixed" AND tech="GWAS" '
                                    f'ORDER BY subjects DESC')
            rows = connection.execute(query).all()
        print(f'Returned {len(rows)} rows for largest mixed dataset')
        return [f'{row[0]}/{row[1]}' for row in rows]


def get_s3_dirs(phenotype, ancestry):
    if ancestry == 'Mixed':
        db = BioIndexDB()
        tech_datasets = db.get_largest_mixed_datasets(phenotype)
        return [f'{s3_in}/variants/{tech_dataset}/{phenotype}/' for tech_dataset in tech_datasets]
    else:
        return [f'{s3_in}/out/metaanalysis/largest/ancestry-specific/{phenotype}/ancestry={ancestry}/']


def get_single_json_file(s3_dir, phenotype, ancestry):
    subprocess.check_call(['aws', 's3', 'cp', s3_dir, f'{phenotype_files}/', '--recursive', '--exclude=_SUCCESS', '--exclude=metadata'])
    with open(f'{phenotype_files}/{phenotype}_{ancestry}.json', 'w') as f_out:
        for file in glob.glob(f'{phenotype_files}/part-*', recursive=True):
            filename, file_extension = os.path.splitext(file)
            if file_extension == '.zst':
                subprocess.check_call(['zstd', '-d', file])
                file = filename  # zst gone from resulting file to use
            with open(file, 'r') as f_in:
                shutil.copyfileobj(f_in, f_out)
            os.remove(file)


def upload(phenotype, ancestry):
    s3_dir = f'{s3_out}/out/ldsc/sumstats/{phenotype}/ancestry={ancestry}/'
    subprocess.check_call(['aws', 's3', 'cp', f'{phenotype_files}/{phenotype}_{ancestry}.log', s3_dir])
    subprocess.check_call(['aws', 's3', 'cp', f'{phenotype_files}/{phenotype}_{ancestry}.sumstats.gz', s3_dir])


def remove_files(phenotype, ancestry):
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
    line_count = 0
    with open(f'{phenotype_files}/{phenotype}_{ancestry}.json', 'r') as f_in:
        with open(f'{phenotype_files}/{phenotype}_{ancestry}.txt', 'w') as f_out:
            line_template = '{}\t{}\t{}\t{}\t{}\t{}\n'
            f_out.write(line_template.format('MarkerName', 'Allele1', 'Allele2', 'p', 'beta', 'N'))
            json_string = f_in.readline()
            while len(json_string) > 0:
                for json_substring in json_string.replace('}{', '}\n{').splitlines():
                    line = json.loads(json_substring)
                    if 'varId' in line and \
                            line['varId'] in snp_map \
                            and line['beta'] is not None and \
                            line['chromosome'] in valid_chromosomes:
                        line_string = line_template.format(
                            snp_map[line['varId']],
                            line['reference'].lower(),
                            line['alt'].lower(),
                            line['pValue'],
                            line['beta'],
                            line['n']
                        )
                        f_out.write(line_string)
                        line_count += 1
                json_string = f_in.readline()
    return line_count


def create_sumstats(phenotype, ancestry):
    subprocess.check_call([
        'python3', f'{ldsc_files}/munge_sumstats.py',
        '--sumstats', f'{phenotype_files}/{phenotype}_{ancestry}.txt',
        '--out', f'{phenotype_files}/{phenotype}_{ancestry}',
        '--merge-alleles', f'{snp_file}/w_hm3.snplist'
    ])


def run(phenotype, ancestry, snp_map):
    for s3_dir in get_s3_dirs(phenotype, ancestry):
        print(f'Using directory: {s3_dir}')
        get_single_json_file(s3_dir, phenotype, ancestry)
        total_lines = stream_to_txt(phenotype, ancestry, snp_map)
        if total_lines > 0:
            create_sumstats(phenotype, ancestry)
            upload(phenotype, ancestry)
            return True
        remove_files(phenotype, ancestry)


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

    snp_map = get_snp_map()
    print(f'Created SNP map ({len(snp_map)} variants)')

    run(phenotype, ancestry, snp_map)
    remove_files(phenotype, ancestry)


if __name__ == '__main__':
    main()
