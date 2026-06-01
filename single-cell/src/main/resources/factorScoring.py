#!/usr/bin/python3
import argparse
import os
import shutil
import subprocess

downloaded_files = '/mnt/var/pigean'
s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']


def get_gene_loading_data(dataset, cell_type, model):
    file_in = f'{s3_in}/out/single_cell/staging/factor_matrix/{dataset}/{cell_type}/{model}/factor_matrix_gene_loadings.tsv'
    gene_loading_data = {}
    if subprocess.call(['aws', 's3', 'ls', f'{file_in}']) == 0:
        subprocess.check_call(['aws', 's3', 'cp', f'{file_in}', 'inputs/'])
        with open('inputs/factor_matrix_gene_loadings.tsv', 'r') as f:
            header = f.readline().strip().split('\t')
            for factor_key in header[1:]:
                gene_loading_data[factor_key] = []
            for line in f:
                gene, factor_data = line.strip().split('\t', 1)
                for factor_key, factor_value in dict(zip(header[1:], factor_data.split('\t'))).items():
                    gene_loading_data[factor_key].append((float(factor_value), gene))
    top_250_genes = {}
    for factor_key, gene_data in gene_loading_data.items():
        top_250_genes[factor_key] = [gene_datum[1] for gene_datum in sorted(gene_data, reverse=True)[:250]]
    return top_250_genes


def make_gmt(dataset, cell_type, model, factor_key, gene_data):
    with open('factor.gmt', 'w') as f:
        f.write('{}\t{}\t{}\n'.format(
            f'{dataset}.{cell_type}.{model}.{factor_key}',
            f'{dataset} {cell_type} {model} {factor_key}',
            '\t'.join(gene_data)
        ))


def run_betas(dataset, cell_type, model, top_250_genes):
    for factor_key, gene_data in top_250_genes.items():
        make_gmt(dataset, cell_type, model, factor_key, gene_data)
        cmd = ['python3.11', '-m', 'pigean', 'betas',
                '--X-in', f'factor.gmt',
                '--multi-y-in', f'{downloaded_files}/gs_{model}.tsv',
                '--multi-y-id-col', 'gene',
                '--multi-y-pheno-col', 'trait',
                '--multi-y-log-bf-col', 'log_bf',
                '--multi-y-combined-col', 'combined',
                '--multi-y-prior-col', 'huge',
                '--multi-y-trait-blacklist-in', 'trait_blacklist_exomes_hp.txt',
                '--gene-universe-in', f'{downloaded_files}/NCBI37.3.plink.gene.loc',
                '--gene-universe-id-col', '6',
                '--gene-universe-no-header',
                '--gene-set-stats-out', os.path.abspath(f'outputs/gss.{factor_key}.out'),
                '--deterministic',
                '--min-gene-set-size', '1',
                '--filter-gene-set-p', '1',
                '--max-gene-set-read-p', '1' ,
                '--no-filter-negative',
                '--prune-gene-sets', '1.1',
                '--weighted-prune-gene-sets', '1.1'
               ]
        subprocess.run(cmd, cwd=f'{downloaded_files}/pigean/src')


def upload(dataset, cell_type, model):
    path_out = f'{s3_out}/out/single_cell/staging/factor_scoring/{dataset}/{cell_type}/{model}'
    subprocess.check_call(['aws', 's3', 'cp', f'output/', f'{path_out}', '--recursive'])


def main():
    opts = argparse.ArgumentParser()
    opts.add_argument('--dataset', type=str, required=True)
    opts.add_argument('--cell-type', type=str, required=True)
    opts.add_argument('--model', type=str, required=True)
    args = opts.parse_args()

    os.makedirs('outputs', exist_ok=True)
    top_250_genes = get_gene_loading_data(args.dataset, args.cell_type, args.model)
    run_betas(args.dataset, args.cell_type, args.model, top_250_genes)
    upload(args.dataset, args.cell_type, args.model)
    shutil.rmtree('inputs')
    shutil.rmtree('outputs')


if __name__ == '__main__':
    main()
