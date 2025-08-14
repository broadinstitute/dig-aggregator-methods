#!/usr/bin/python3
import argparse
from boto3.session import Session
import json
import os
import shutil
import subprocess

downloaded_files = '/mnt/var/pigean'
s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']

gene_sets = {
    'gene_set_list_mouse_2024.txt': ['small', 'large', 'cfde', 'mouse'],
    'gene_set_list_msigdb_nohp.txt': ['small', 'large', 'cfde', 'ryank061025'],
    'gene_set_list_string_notext_medium_processed.txt': ['large'],
    'gene_set_list_pops_sparse_small.txt': ['large'],
    'gene_set_list_mesh_processed.txt': ['large'],
    'mouse_disease_ontology_to_mouse_gene_to_human_gene_map.Alliance.gmt': ['ryank061025'],
    'mouse_disease_ontology_to_mouse_gene_to_human_gene_map.Alliance.one_to_one_ortho.gmt': ['ryank061025'],
    'mouse_disease_ontology_to_mouse_gene_to_human_gene_map.Alliance.one_to_one_ortho.pairs.intersections.gmt': ['ryank061025'],
    'mouse_disease_ontology_to_mouse_gene_to_human_gene_map.Alliance.pairs.intersections.gmt': ['ryank061025'],
    'mouse_disease_ontology_to_mouse_gene_to_human_gene_map.MGI.gmt': ['ryank061025'],
    'mouse_disease_ontology_to_mouse_gene_to_human_gene_map.MGI.one_to_one_ortho.gmt': ['ryank061025'],
    'mouse_disease_ontology_to_mouse_gene_to_human_gene_map.MGI.one_to_one_ortho.pairs.intersections.gmt': ['ryank061025'],
    'mouse_disease_ontology_to_mouse_gene_to_human_gene_map.MGI.pairs.intersections.gmt': ['ryank061025'],
    'mouse_pheno_to_mouse_gene_to_human_gene_map.IMPC_3I.gmt': ['ryank061025'],
    'mouse_pheno_to_mouse_gene_to_human_gene_map.IMPC_3I.one_to_one_ortho.gmt': ['ryank061025'],
    'mouse_pheno_to_mouse_gene_to_human_gene_map.IMPC_3I.one_to_one_ortho.pairs.intersections.gmt': ['ryank061025'],
    'mouse_pheno_to_mouse_gene_to_human_gene_map.IMPC_3I.pairs.intersections.gmt': ['ryank061025'],
    'mouse_pheno_to_mouse_gene_to_human_gene_map.IMPC_ALL.gmt': ['ryank061025'],
    'mouse_pheno_to_mouse_gene_to_human_gene_map.IMPC_ALL.one_to_one_ortho.gmt': ['ryank061025'],
    'mouse_pheno_to_mouse_gene_to_human_gene_map.IMPC_ALL.one_to_one_ortho.pairs.intersections.gmt': ['ryank061025'],
    'mouse_pheno_to_mouse_gene_to_human_gene_map.IMPC_ALL.pairs.intersections.gmt': ['ryank061025'],
    'mouse_pheno_to_mouse_gene_to_human_gene_map.IMPC_EUROPHENOME.gmt': ['ryank061025'],
    'mouse_pheno_to_mouse_gene_to_human_gene_map.IMPC_EUROPHENOME.one_to_one_ortho.gmt': ['ryank061025'],
    'mouse_pheno_to_mouse_gene_to_human_gene_map.IMPC_EUROPHENOME.one_to_one_ortho.pairs.intersections.gmt': ['ryank061025'],
    'mouse_pheno_to_mouse_gene_to_human_gene_map.IMPC_EUROPHENOME.pairs.intersections.gmt': ['ryank061025'],
    'mouse_pheno_to_mouse_gene_to_human_gene_map.IMPC_IMPC.gmt': ['ryank061025'],
    'mouse_pheno_to_mouse_gene_to_human_gene_map.IMPC_IMPC.one_to_one_ortho.gmt': ['ryank061025'],
    'mouse_pheno_to_mouse_gene_to_human_gene_map.IMPC_IMPC.one_to_one_ortho.pairs.intersections.gmt': ['ryank061025'],
    'mouse_pheno_to_mouse_gene_to_human_gene_map.IMPC_IMPC.pairs.intersections.gmt': ['ryank061025'],
    'mouse_pheno_to_mouse_gene_to_human_gene_map.IMPC_MGP.gmt': ['ryank061025'],
    'mouse_pheno_to_mouse_gene_to_human_gene_map.IMPC_MGP.one_to_one_ortho.gmt': ['ryank061025'],
    'mouse_pheno_to_mouse_gene_to_human_gene_map.IMPC_MGP.one_to_one_ortho.pairs.intersections.gmt': ['ryank061025'],
    'mouse_pheno_to_mouse_gene_to_human_gene_map.IMPC_MGP.pairs.intersections.gmt': ['ryank061025'],
    'mouse_pheno_to_mouse_gene_to_human_gene_map.MGI.gmt': ['ryank061025'],
    'mouse_pheno_to_mouse_gene_to_human_gene_map.MGI.one_to_one_ortho.gmt': ['ryank061025'],
    'mouse_pheno_to_mouse_gene_to_human_gene_map.MGI.one_to_one_ortho.pairs.intersections.gmt': ['ryank061025'],
    'mouse_pheno_to_mouse_gene_to_human_gene_map.MGI.pairs.intersections.gmt': ['ryank061025'],
    'mouse_top_level_pheno_to_mouse_gene_to_human_gene_map.IMPC_3I.gmt': ['ryank061025'],
    'mouse_top_level_pheno_to_mouse_gene_to_human_gene_map.IMPC_3I.one_to_one_ortho.gmt': ['ryank061025'],
    'mouse_top_level_pheno_to_mouse_gene_to_human_gene_map.IMPC_3I.one_to_one_ortho.pairs.intersections.gmt': ['ryank061025'],
    'mouse_top_level_pheno_to_mouse_gene_to_human_gene_map.IMPC_3I.pairs.intersections.gmt': ['ryank061025'],
    'mouse_top_level_pheno_to_mouse_gene_to_human_gene_map.IMPC_ALL.gmt': ['ryank061025'],
    'mouse_top_level_pheno_to_mouse_gene_to_human_gene_map.IMPC_ALL.one_to_one_ortho.gmt': ['ryank061025'],
    'mouse_top_level_pheno_to_mouse_gene_to_human_gene_map.IMPC_ALL.one_to_one_ortho.pairs.intersections.gmt': ['ryank061025'],
    'mouse_top_level_pheno_to_mouse_gene_to_human_gene_map.IMPC_ALL.pairs.intersections.gmt': ['ryank061025'],
    'mouse_top_level_pheno_to_mouse_gene_to_human_gene_map.IMPC_EUROPHENOME.gmt': ['ryank061025'],
    'mouse_top_level_pheno_to_mouse_gene_to_human_gene_map.IMPC_EUROPHENOME.one_to_one_ortho.gmt': ['ryank061025'],
    'mouse_top_level_pheno_to_mouse_gene_to_human_gene_map.IMPC_EUROPHENOME.one_to_one_ortho.pairs.intersections.gmt': ['ryank061025'],
    'mouse_top_level_pheno_to_mouse_gene_to_human_gene_map.IMPC_EUROPHENOME.pairs.intersections.gmt': ['ryank061025'],
    'mouse_top_level_pheno_to_mouse_gene_to_human_gene_map.IMPC_IMPC.gmt': ['ryank061025'],
    'mouse_top_level_pheno_to_mouse_gene_to_human_gene_map.IMPC_IMPC.one_to_one_ortho.gmt': ['ryank061025'],
    'mouse_top_level_pheno_to_mouse_gene_to_human_gene_map.IMPC_IMPC.one_to_one_ortho.pairs.intersections.gmt': ['ryank061025'],
    'mouse_top_level_pheno_to_mouse_gene_to_human_gene_map.IMPC_IMPC.pairs.intersections.gmt': ['ryank061025'],
    'mouse_top_level_pheno_to_mouse_gene_to_human_gene_map.IMPC_MGP.gmt': ['ryank061025'],
    'mouse_top_level_pheno_to_mouse_gene_to_human_gene_map.IMPC_MGP.one_to_one_ortho.gmt': ['ryank061025'],
    'mouse_top_level_pheno_to_mouse_gene_to_human_gene_map.IMPC_MGP.one_to_one_ortho.pairs.intersections.gmt': ['ryank061025'],
    'mouse_top_level_pheno_to_mouse_gene_to_human_gene_map.IMPC_MGP.pairs.intersections.gmt': ['ryank061025']
}

gene_lists = {
    'cfde.gene_sets.list': ['cfde'],
    'cfde_overlap.gene_sets.list': ['cfde'],
    'mouse.gene_sets.list': ['mouse']
}


class OpenAPIKey:
    def __init__(self):
        self.secret_id = 'openapi-key'
        self.region = 'us-east-1'
        self.config = None

    def get_config(self):
        if self.config is None:
            client = Session().client('secretsmanager', region_name=self.region)
            self.config = json.loads(client.get_secret_value(SecretId=self.secret_id)['SecretString'])
        return self.config

    def get_key(self):
        if self.config is None:
            self.config = self.get_config()
        return self.config['apiKey']


def download_data(trait_group, phenotype, gene_set_size):
    file_path = f'{s3_in}/out/pigean/staging/pigean/{trait_group}/{phenotype}/{gene_set_size}'
    subprocess.check_call(['aws', 's3', 'cp', f'{file_path}/gs.out', '.'])
    subprocess.check_call(['aws', 's3', 'cp', f'{file_path}/gss.out', '.'])


def download_combined_data(gene_set_size):
    file_path = f'{s3_in}/out/pigean/staging/combined'
    subprocess.check_call(['aws', 's3', 'cp', f'{file_path}/gs_{gene_set_size}.tsv', 'combined/'])
    subprocess.check_call(['aws', 's3', 'cp', f'{file_path}/gss_{gene_set_size}.tsv', 'combined/'])


def get_gene_sets(gene_set_size):
    size_gene_sets = [gene_set for gene_set, sizes in gene_sets.items() if gene_set_size in sizes]
    size_gene_lists = [gene_list for gene_list, sizes in gene_lists.items() if gene_set_size in sizes]
    all_inputs = ([cmd for gene_set in size_gene_sets for cmd in ('--X-in', f'{downloaded_files}/{gene_set}')] +
                  [cmd for gene_set_list in size_gene_lists for cmd in ('--X-list', f'{downloaded_files}/{gene_set_list}')])
    if len(all_inputs) > 0:
        return all_inputs
    else:
        raise Exception(f'Invalid gene set size {gene_set_size}')


def run_factor(gene_set_size, phi, openapi_key):
    cmd = [
              'python3', f'{downloaded_files}/priors-202507.py', 'factor',
              '--phi', f'0.0{phi}',
              '--gene-set-stats-in', 'gss.out',
              '--gene-stats-in', 'gs.out',
              '--gene-map-in', f'{downloaded_files}/gencode.gene.map',
              '--gene-set-phewas-stats-in', f'combined/gss_{gene_set_size}.tsv',
              '--gene-set-phewas-stats-id-col', 'gene_set',
              '--gene-set-phewas-stats-pheno-col', 'trait',
              '--gene-phewas-stats-in', f'combined/gs_{gene_set_size}.tsv',
              '--run-phewas-from-gene-phewas-stats-in', f'combined/gs_{gene_set_size}.tsv',
              '--gene-phewas-bfs-id-col', 'gene',
              '--gene-phewas-bfs-pheno-col', 'trait',
              '--gene-phewas-bfs-combined-col', 'combined',
              '--gene-phewas-bfs-log-bf-col', 'log_bf',
              '--max-num-gene-sets', '5000',
              '--phewas-stats-out', 'phs.out',
              '--factors-out', 'f.out',
              '--factors-anchor-out', 'fa.out',
              '--gene-clusters-out', 'gc.out',
              '--gene-anchor-clusters-out', 'gac.out',
              '--pheno-clusters-out', 'pc.out',
              '--pheno-anchor-clusters-out', 'pac.out',
              '--gene-set-clusters-out', 'gsc.out',
              '--gene-set-anchor-clusters-out', 'gsac.out',
              '--params-out', 'p.out'
          ] + get_gene_sets(gene_set_size) + \
          (['--lmm-auth-key', openapi_key] if openapi_key is not None else [])
    subprocess.check_call(cmd)


def success(file_path):
    subprocess.check_call(['touch', '_SUCCESS'])
    subprocess.check_call(['aws', 's3', 'cp', '_SUCCESS', file_path])
    os.remove('_SUCCESS')


def upload_data(trait_group, phenotype, gene_set_size, phi):
    file_path = f'{s3_out}/out/pigean/staging/factor/{trait_group}/{phenotype}/{gene_set_size}___phi{phi}/'
    for file in ['phs.out', 'f.out', 'fa.out', 'gc.out', 'gac.out', 'pc.out', 'pac.out', 'gsc.out', 'gsac.out', 'p.out']:
        if os.path.exists(file):
            subprocess.check_call(['aws', 's3', 'cp', file, file_path])
            os.remove(file)
    success(file_path)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--trait-group', default=None, required=True, type=str,
                        help="Input phenotype group.")
    parser.add_argument('--phenotype', default=None, required=True, type=str,
                        help="Input phenotype.")
    parser.add_argument('--gene-set-size', default=None, required=True, type=str,
                        help="gene-set-size (e.g. small)")
    parser.add_argument('--phi', default=None, required=True, type=str,
                        help="phi (e.g. 3 corresponding to 0.03).")
    args = parser.parse_args()

    open_api_key = OpenAPIKey().get_key()
    download_data(args.trait_group, args.phenotype, args.gene_set_size)
    download_combined_data(args.gene_set_size)
    try:
        run_factor(args.gene_set_size, args.phi, open_api_key)
        upload_data(args.trait_group, args.phenotype, args.gene_set_size, args.phi)
    except Exception:
        print('Error')
    os.remove('gs.out')
    os.remove('gss.out')
    shutil.rmtree('combined')


if __name__ == '__main__':
    main()



