#!/usr/bin/python3
import argparse
import json
import os
import subprocess

downloaded_files = '/mnt/var/pigean'
s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']

def get_model_data():
    with open(f'{downloaded_files}/aws_pigean_models_s3.json', 'r') as f:
        models = json.load(f)
    return ({model['name']: model for model in models['models']},
            {gene_set['name']: gene_set for gene_set in models['gene_sets']})


def file_names(trait_type):
    if trait_type == 'sumstats':
        return ['pigean.sumstats.gz']
    elif trait_type == 'gene_lists':
        return ['gene_list.tsv']
    elif trait_type == 'exomes':
        return ['exomes.sumstats.gz']
    elif trait_type == 'exomes___sumstats':
        return ['exomes.sumstats.gz', 'pigean.sumstats.gz']
    else:
        raise ValueError(f'Invalid trait_type: {trait_type}')


def download_data(trait_type, trait_group, phenotype):
    for file_name in file_names(trait_type):
        file_path = f'{s3_in}/out/pigean/inputs/{trait_type}/{trait_group}/{phenotype}/{file_name}'
        subprocess.check_call(['aws', 's3', 'cp', file_path, '.'])


def get_gene_sets(gene_set_size):
    models, gene_sets = get_model_data()
    model_info = models[gene_set_size]
    inputs = []
    p_infs = []
    for gene_set in model_info['gene_sets']:
        gene_set_info = gene_sets[gene_set]
        if gene_set_info['type'] == 'set':
            inputs += ['--X-in', f'{downloaded_files}/{gene_set_info["file"]}']
        else:
            inputs += ['--X-list', f'{downloaded_files}/{gene_set_info["name"]}/{gene_set_info["file"]}']
        p_infs += ['--p-noninf', str(gene_set_info['p-inf'])]
    if len(inputs) > 0:
        return inputs + p_infs
    else:
        raise Exception(f'Invalid gene set size {gene_set_size}')


def trait_type_command(trait_type):
    cmds = []
    files_names = file_names(trait_type)
    for file_name in files_names:
        if file_name == 'pigean.sumstats.gz':
            cmds += ['--gwas-in', os.path.abspath(file_name),
                    '--gwas-chrom-col', 'CHROM',
                    '--gwas-pos-col', 'POS',
                    '--gwas-p-col', 'P',
                    '--gwas-n-col', 'N'
                    ]
        elif file_name == 'gene_list.tsv':
            cmds += [
                '--positive-controls-in', os.path.abspath(file_name),
                '--positive-controls-id-col', '1',
                '--positive-controls-prob-col', '2',
                '--positive-controls-no-header', 'True',
                '--positive-controls-all-in', f'{downloaded_files}/refGene_hg19_TSS.subset.loc',
                '--positive-controls-all-no-header', 'True',
                '--positive-controls-all-id-col', '1'
            ]
        elif file_name == 'exomes.sumstats.gz':
            cmds += [
                '--exomes-in', os.path.abspath(file_name),
                '--exomes-gene-col', 'Gene',
                '--exomes-p-col', 'P-value',
                '--exomes-beta-col', 'Effect'
            ]
    return cmds


base_cmd = [
    'python3.11', '-m', 'pigean', 'gibbs',
    '--gene-loc-file', f'{downloaded_files}/NCBI37.3.plink.gene.loc',
    '--gene-map-in', f'{downloaded_files}/portal_gencode.gene.map',
    '--gene-loc-file-huge', f'{downloaded_files}/refGene_hg19_TSS.subset.loc',
    '--exons-loc-file-huge', f'{downloaded_files}/NCBI37.3.plink.gene.exons.loc',
    '--gene-universe-in', f'{downloaded_files}/gene-universe.txt',
    '--gene-stats-out', os.path.abspath('outputs/gs.out'),
    '--gene-set-stats-out', os.path.abspath('outputs/gss.out'),
    '--gene-gene-set-stats-out', os.path.abspath('outputs/ggss.out'),
    '--gene-effectors-out', os.path.abspath('outputs/ge.out'),
    '--params-out', os.path.abspath('outputs/p.out')
]


def run_pigean(trait_type, phenotype, gene_set_size):
    os.makedirs('outputs', exist_ok=True)
    cmd = base_cmd + trait_type_command(trait_type) + get_gene_sets(gene_set_size)
    subprocess.check_call(cmd, cwd=f'{downloaded_files}/pigean/src')


def success(file_path):
    subprocess.check_call(['touch', '_SUCCESS'])
    subprocess.check_call(['aws', 's3', 'cp', '_SUCCESS', file_path])
    os.remove('_SUCCESS')


def upload(file_name, file_path):
    if os.path.exists(file_name):
        subprocess.check_call(['aws', 's3', 'cp', file_name, file_path])
        os.remove(file_name)

def upload_data(trait_type, trait_group, phenotype, gene_set_size):
    file_path = f'{s3_out}/out/pigean/staging/pigean/{trait_group}/{phenotype}/{gene_set_size}/'
    upload('outputs/gs.out', file_path)
    upload('outputs/gss.out', file_path)
    upload('outputs/ggss.out', file_path)
    upload('outputs/ge.out', file_path)
    success(file_path)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--trait-type', default=None, required=True, type=str,
                        help="sumstats or gene_lists")
    parser.add_argument('--trait-group', default=None, required=True, type=str,
                        help="Trait group")
    parser.add_argument('--phenotype', default=None, required=True, type=str,
                        help="Input phenotype.")
    parser.add_argument('--gene-set-size', default=None, required=True, type=str,
                        help="gene-set-size (small, medium, or large).")
    args = parser.parse_args()
    download_data(args.trait_type, args.trait_group, args.phenotype)
    try:
        run_pigean(args.trait_type, args.phenotype, args.gene_set_size)
        upload_data(args.trait_type, args.trait_group, args.phenotype, args.gene_set_size)
        for file_name in file_names(args.trait_type):
            os.remove(file_name)
    except:
        print('ERROR')


if __name__ == '__main__':
    main()


