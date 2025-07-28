#!/usr/bin/python3
import argparse
from boto3 import session
import glob
import gzip
import json
import numpy as np
import os
import shutil
from scipy.stats import chi2
import sqlalchemy
import subprocess
from typing import List, Dict, Optional

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
phenotype_files = '.'
snpmap_files = f'{downloaded_files}/snpmap'
weights_files = f'{downloaded_files}/weights'

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


def weights_path(ancestry: str, chromosome: int) -> str:
    return f'{weights_files}/{ancestry_map[ancestry]}/weights.{chromosome}.l2.ldscore.gz'


def get_var_to_rs_map(ancestry: str, build_type: str) -> Dict:
    var_to_rs = {}
    with open(f'{snpmap_files}/sumstats.{build_type}.GRCh37.{ancestry_map[ancestry]}.snpmap', 'r') as f:
        for full_row in f.readlines():
            var_id, rs_id = full_row.strip().split('\t')
            var_to_rs[var_id] = rs_id
    return var_to_rs


def get_s3_dirs(phenotype, ancestry):
    if ancestry == 'Mixed':
        db = BioIndexDB()
        tech_datasets = db.get_largest_mixed_datasets(phenotype)
        return [f'{s3_in}/variants/{tech_dataset}/{phenotype}/' for tech_dataset in tech_datasets]
    else:
        return [f'{s3_in}/out/metaanalysis/bottom-line/ancestry-specific/{phenotype}/ancestry={ancestry}/']


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


def p_to_z(p: float, beta: float) -> float:
    return np.sqrt(chi2.isf(p, 1)) * (-1)**(beta < 0)


def valid_line(line: Dict) -> bool:
    return ('pValue' in line and
            line['pValue'] != '' and
            'beta' in line and
            'n' in line and
            0 < float(line['pValue']) <= 1)


def stream_to_data(file_path: str, var_to_rs_map: Dict, var_to_rs_flipped: Dict) -> (List, Dict):
    out = []
    counts = {'all': 0, 'flipped': 0, 'error': 0}
    with open(file_path, 'r') as f_in:
        for json_string in f_in:
            line = json.loads(json_string.strip())
            counts['all'] += 1
            if valid_line(line):
                var_id = line['varId']
                if var_id in var_to_rs_map or var_id in var_to_rs_flipped:
                    flipped = var_id in var_to_rs_flipped
                    rs_id = var_to_rs_flipped[var_id] if flipped else var_to_rs_map[var_id]
                    try:
                        p_value = float(line['pValue'])
                        beta = float(line['beta']) * (1 - 2 * flipped)
                        n = float(line['n'])
                        out.append((rs_id, p_to_z(p_value, beta), n))
                        counts['flipped'] += flipped
                    except ValueError:  # pValue, beta, or n not a value that can be converted to a float, skip
                        counts['error'] += 1
    counts['translated'] = len(out)
    return out, counts


def filter_data_to_dict(data: List) -> Dict:
    N90 = np.quantile([a[2] for a in data], 0.9)
    return {d[0]: (d[1], d[2]) for d in data if d[2] >= N90 / 1.5}


def save_to_file(file: str, ancestry: str, data: Dict, log: Dict) -> Dict:
    with_data = 0
    with gzip.open(file, 'wt') as f:
        f.write('SNP\tZ\tN\n')
        for rs_id in ld_rs_iter(ancestry):
            if rs_id in data:
                with_data += 1
                f.write('{}\t{}\t{}\n'.format(rs_id, round(data[rs_id][0], 3), data[rs_id][1]))
            else:
                f.write(f'{rs_id}\t\t\n')
    log['final'] = with_data
    return log


def ld_rs_iter(ancestry: str) -> str:
    for chromosome in range(1, 23):
        with gzip.open(weights_path(ancestry, chromosome), 'rt') as f:
            _ = f.readline()
            for line in f:
                yield line.strip().split('\t')[1]


def remove_files(phenotype, ancestry):
    for file in glob.glob(f'{phenotype_files}/{phenotype}_{ancestry}.*'):
        os.remove(file)


def upload(phenotype, ancestry):
    s3_dir = f'{s3_out}/out/ldsc/server_sumstats/{phenotype}/ancestry={ancestry}/'
    subprocess.check_call(['aws', 's3', 'cp', f'{phenotype_files}/{phenotype}_{ancestry}.log', s3_dir])
    subprocess.check_call(['aws', 's3', 'cp', f'{phenotype_files}/{phenotype}_{ancestry}.sumstats.gz', s3_dir])


def run(phenotype, ancestry, var_to_rs_map, var_to_rs_flipped):
    for s3_dir in get_s3_dirs(phenotype, ancestry):
        print(f'Using directory: {s3_dir}')
        get_single_json_file(s3_dir, phenotype, ancestry)
        data, log = stream_to_data(f'{phenotype_files}/{phenotype}_{ancestry}.json', var_to_rs_map, var_to_rs_flipped)
        if len(data) > 0:
            data_dict = filter_data_to_dict(data)
            log['pass_filter'] = len(data_dict)
            log = save_to_file(f'{phenotype_files}/{phenotype}_{ancestry}.sumstats.gz', ancestry, data_dict, log)
            if log['final'] > 10000:
                with open(f'{phenotype_files}/{phenotype}_{ancestry}.log', 'w') as f:
                    json.dump(log, f)
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

    var_to_rs_map = get_var_to_rs_map(ancestry, 'standard')
    var_to_rs_flipped = get_var_to_rs_map(ancestry, 'flipped')
    run(phenotype, ancestry, var_to_rs_map, var_to_rs_flipped)
    remove_files(phenotype, ancestry)


if __name__ == '__main__':
    main()
