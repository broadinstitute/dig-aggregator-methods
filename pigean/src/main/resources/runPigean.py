#!/usr/bin/python3
import argparse
import os
import subprocess

downloaded_files = '/mnt/var/pigean'
s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']

size_sorting = {
    'gene_set_list_mouse.txt': ['small', 'large'],
    'gene_set_list_msigdb_nohp.txt': ['small', 'large'],
    'gene_set_list_string_notext_medium_processed.txt': ['large'],
    'gene_set_list_pops_sparse_small.txt': ['large'],
    'gene_set_list_mesh_processed.txt': ['large']
}


def download_data(phenotype):
    file_path = f'{s3_in}/out/pigean/inputs/sumstats/{phenotype}/{phenotype}.sumstats.gz'
    subprocess.check_call(['aws', 's3', 'cp', file_path, '.'])


def get_gene_sets(gene_set_size):
    size_gene_sets = [gene_set for gene_set, sizes in size_sorting.items() if gene_set_size in sizes]
    if len(size_gene_sets) > 0:
        return [cmd for gene_set in size_gene_sets for cmd in ('--X-in', f'{downloaded_files}/{gene_set}')]
    else:
        raise Exception(f'Invalid gene set size {gene_set_size}')


def run_pigean(phenotype, sigma, gene_set_size):
    cmd = [
        'python3', f'{downloaded_files}/priors.py', 'gibbs',
        '--first-for-sigma-cond',
        '--sigma-power', f'-{sigma}',
        '--gwas-detect-high-power', '100',
        '--gwas-detect-low-power', '10',
        '--num-chains', '10',
        '--num-chains-betas', '4',
        '--max-num-iter', '500',
        '--background-prior', '0.05',
        '--filter-gene-set-p', '0.005',
        '--max-num-gene-sets', '4000',
        '--gwas-in', f'{phenotype}.sumstats.gz',
        '--gwas-chrom-col', 'CHROM',
        '--gwas-pos-col', 'POS',
        '--gwas-p-col', 'P',
        '--gwas-n-col', 'N',
        '--exomes-gene-col', 'Gene',
        '--exomes-p-col', 'P-value',
        '--exomes-beta-col', 'Effect',
        '--gene-loc-file', f'{downloaded_files}/NCBI37.3.plink.gene.loc',
        '--gene-map-in', f'{downloaded_files}/gencode.gene.map',
        '--gene-loc-file-huge', f'{downloaded_files}/refGene_hg19_TSS.subset.loc',
        '--exons-loc-file-huge', f'{downloaded_files}/NCBI37.3.plink.gene.exons.loc',
        '--gene-stats-out', 'gs.out',
        '--gene-set-stats-out', 'gss.out',
        '--gene-gene-set-stats-out', 'ggss.out',
    ] + get_gene_sets(gene_set_size)
    subprocess.check_call(cmd)


def success(file_path):
    subprocess.check_call(['touch', '_SUCCESS'])
    subprocess.check_call(['aws', 's3', 'cp', '_SUCCESS', file_path])
    os.remove('_SUCCESS')


def upload_data(phenotype, sigma, gene_set_size):
    file_path = f'{s3_out}/out/pigean/staging/pigean/{phenotype}/sigma={sigma}/size={gene_set_size}/'
    subprocess.check_call(['aws', 's3', 'cp', 'gs.out', file_path])
    subprocess.check_call(['aws', 's3', 'cp', 'gss.out', file_path])
    subprocess.check_call(['aws', 's3', 'cp', 'ggss.out', file_path])
    success(file_path)
    os.remove('gs.out')
    os.remove('gss.out')
    os.remove('ggss.out')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--phenotype', default=None, required=True, type=str,
                        help="Input phenotype.")
    parser.add_argument('--sigma', default=None, required=True, type=str,
                        help="Sigma power (0, 2, 4).")
    parser.add_argument('--gene-set-size', default=None, required=True, type=str,
                        help="gene-set-size (small, medium, or large).")
    args = parser.parse_args()
    download_data(args.phenotype)
    try:
        run_pigean(args.phenotype, args.sigma, args.gene_set_size)
        upload_data(args.phenotype, args.sigma, args.gene_set_size)
        os.remove(f'{args.phenotype}.sumstats.gz')
    except:
        print('ERROR')


if __name__ == '__main__':
    main()


