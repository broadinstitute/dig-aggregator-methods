#!/usr/bin/python3
import argparse
import glob
import json
import os
import shutil
import subprocess


downloaded_files = '/mnt/var/pigean'
s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']


model_to_gene_stats = {
    'mouse_msigdb_phi1': 'mouse_msigdb',
    'mouse_msigdb_phi5': 'mouse_msigdb',
    'mouse_msigdb': 'mouse_msigdb'
}


def download_files(dataset, cell_type, model):
    file = f'{s3_in}/out/single_cell/staging/factor_matrix/{dataset}/{cell_type}/{model}/factor_matrix_gene_probs.tsv'
    subprocess.check_call(['aws', 's3', 'cp', f'{file}', 'input/'])


def get_model_data():
    with open(f'{downloaded_files}/aws_pigean_models_s3.json', 'r') as f:
        models = json.load(f)
    return ({model['name']: model for model in models['models']},
            {gene_set['name']: gene_set for gene_set in models['gene_sets']})


def get_gene_sets(gene_set_size):
    models, gene_sets = get_model_data()
    model_info = models[gene_set_size]
    inputs = []
    for gene_set in model_info['gene_sets']:
        gene_set_info = gene_sets[gene_set]
        if gene_set_info['type'] == 'set':
            inputs += ['--X-in', f'{downloaded_files}/{gene_set_info["file"]}']
        else:
            inputs += ['--X-list', f'{downloaded_files}/{gene_set_info["name"]}/{gene_set_info["file"]}']
    if len(inputs) > 0:
        return inputs
    else:
        raise Exception(f'Invalid gene set size {gene_set_size}')


def make_positive_controls(idx):
    with open('positive_controls_in.txt', 'w') as f_out:
        f_out.write('gene\tprob\n')
        with open('input/factor_matrix_gene_probs.tsv', 'r') as f:
            _ = f.readline()
            for line in f:
                split_line = line.strip().split('\t')
                prob = 0.05 if float(split_line[idx]) < 0.05 else float(split_line[idx])
                f_out.write(f'{split_line[0]}\t{prob}\n')


def run(model):
    with open('input/factor_matrix_gene_probs.tsv', 'r') as f:
        header = f.readline().strip().split('\t')
    for idx in range(1, len(header)):
        make_positive_controls(idx)
        subprocess.run(['python3.11', '-m', 'pigean', 'gibbs',
                        '--gene-map-in', f'{downloaded_files}/portal_gencode.gene.map',
                        '--max-num-gene-sets', '5000',
                        '--positive-controls-in', os.path.abspath('positive_controls_in.txt'),
                        '--positive-controls-id-col', 'gene',
                        '--positive-controls-prob-col', 'prob',
                        '--positive-controls-all-in', f'{downloaded_files}/NCBI37.3.plink.gene.loc',
                        '--positive-controls-all-id-col', '6',
                        '--positive-controls-all-no-header',
                        '--gene-set-stats-out', os.path.abspath(f'staging/gss.{header[idx]}.out')]
                       + get_gene_sets(model_to_gene_stats[model]),
                       cwd=f'{downloaded_files}/pigean/src')
    os.remove('positive_controls_in.txt')
    return

def combine_gene_sets():
    with open('input/factor_matrix_gene_probs.tsv', 'r') as f:
        num_cols = len(f.readline().strip().split('\t'))

    with open('output/pigean.gene_sets.tsv', 'w') as f_out:
        f_out.write('factor\tgene_set\tbeta\tbeta_uncorrected\n')
        for idx in range(1, num_cols):
            with open(f'staging/gss.Factor{idx}.out', 'r') as f:
                header = f.readline().strip().split('\t')
                for line in f:
                    line_dict = dict(zip(header, line.strip().split('\t')))
                    f_out.write('{}\t{}\t{}\t{}\n'.format(
                        f'Factor{idx}',
                        line_dict['Gene_Set'],
                        line_dict['beta'],
                        line_dict['beta_uncorrected']
                    ))


def upload(dataset, cell_type, model):
    staging_output = f'{s3_out}/out/single_cell/staging/pigean/{dataset}/{cell_type}/{model}'
    subprocess.check_call(['aws', 's3', 'cp', 'staging/', f'{staging_output}/', '--recursive'])

    output = f'{s3_out}/out/single_cell/pigean/{dataset}/{cell_type}/{model}'
    subprocess.check_call(['aws', 's3', 'cp', 'output/', f'{output}/', '--recursive'])


def main():
    opts = argparse.ArgumentParser()
    opts.add_argument('--dataset', type=str, required=True)
    opts.add_argument('--cell-type', type=str, required=True)
    opts.add_argument('--model', type=str, required=True)
    args = opts.parse_args()

    download_files(args.dataset, args.cell_type, args.model)

    os.makedirs('staging', exist_ok=True)
    run(args.model)

    if len(glob.glob('staging/gss.*')) > 0:
        os.makedirs('output', exist_ok=True)
        combine_gene_sets()
        upload(args.dataset, args.cell_type, args.model)
        shutil.rmtree('output')
    shutil.rmtree('input')
    shutil.rmtree('staging')


if __name__ == '__main__':
    main()
