#!/usr/bin/python3
import argparse
import os
import subprocess

downloaded_files = '/mnt/var/pigean'
s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']

gene_sets = {
    'gene_set_list_mouse_2024.txt': ['small', 'large', 'cfde', 'mouse'],
    'gene_set_list_msigdb_nohp.txt': ['small', 'large', 'cfde', 'ryank061025'],
    'gene_set_list_string_notext_medium_processed.txt': ['large'],
    'gene_set_list_pops_sparse_small.txt': ['large'],
    'gene_set_list_mesh_processed.txt': ['large']
}

gene_lists = {
    'cfde/cfde.gene_sets.list': ['cfde'],
    'cfde_overlap/cfde_overlap.gene_sets.list': ['cfde'],
    'mouse/mouse.gene_sets.list': ['mouse'],
    'ryan061025/ryank061025.gene_sets.list': ['ryank061025']
}


def file_name(trait_type):
    if trait_type == 'sumstats':
        return 'pigean.sumstats.gz'
    elif trait_type == 'gene_lists':
        return 'gene_list.tsv'
    elif trait_type == 'exomes':
        return 'exomes.sumstats.gz'
    else:
        raise ValueError(f'Invalid trait_type: {trait_type}')


def download_data(trait_type, trait_group, phenotype):
    file_path = f'{s3_in}/out/pigean/inputs/{trait_type}/{trait_group}/{phenotype}/{file_name(trait_type)}'
    subprocess.check_call(['aws', 's3', 'cp', file_path, '.'])


def get_gene_sets(gene_set_size):
    size_gene_sets = [gene_set for gene_set, sizes in gene_sets.items() if gene_set_size in sizes]
    size_gene_lists = [gene_list for gene_list, sizes in gene_lists.items() if gene_set_size in sizes]
    all_inputs = ([cmd for gene_set in size_gene_sets for cmd in ('--X-in', f'{downloaded_files}/{gene_set}')] +
                  [cmd for gene_set_list in size_gene_lists for cmd in ('--X-list', f'{downloaded_files}/{gene_set_list}')])
    if len(all_inputs) > 0:
        return all_inputs
    else:
        raise Exception(f'Invalid gene set size {gene_set_size}')


def trait_type_command(trait_type):
    if trait_type == 'sumstats':
        return ['--gwas-in', file_name(trait_type),
                '--gwas-chrom-col', 'CHROM',
                '--gwas-pos-col', 'POS',
                '--gwas-p-col', 'P',
                '--gwas-n-col', 'N'
                ]
    elif trait_type == 'gene_lists':
        return [
            '--positive-controls-in', file_name(trait_type),
            '--positive-controls-id-col', '1',
            '--positive-controls-prob-col', '2',
            '--positive-controls-no-header', 'True',
            '--positive-controls-all-in', f'{downloaded_files}/refGene_hg19_TSS.subset.loc',
            '--positive-controls-all-no-header', 'True',
            '--positive-controls-all-id-col', '1'
        ]
    elif trait_type == 'exomes':
        return [
            '--exomes-in', file_name(trait_type),
             '--exomes-gene-col', 'Gene',
             '--exomes-p-col', 'P-value',
             '--exomes-beta-col', 'Effect'
        ]

# NOTE: Removed as model became unstable
def get_background_prior(phenotype):
    # with open(f'{downloaded_files}/code_to_leaves.tsv', 'r') as f:
    #     for line in f:
    #         code, leaves_str = line.strip().split('\t')
    #         if code == phenotype:
    #             return ['--background-prior', str(min(int(leaves_str) * 0.005, 0.05))]
    return ['--background-prior', '0.05']


base_cmd = [
    'python3', f'{downloaded_files}/priors.py', 'gibbs',
    '--first-for-sigma-cond',
    '--sigma-power', f'-2',
    '--gwas-detect-high-power', '100',
    '--gwas-detect-low-power', '10',
    '--num-chains', '10',
    '--num-chains-betas', '4',
    '--max-num-iter', '500',
    '--filter-gene-set-p', '0.005',
    '--max-num-gene-sets', '4000',
    '--min-gene-set-size', '5',
    '--gene-loc-file', f'{downloaded_files}/NCBI37.3.plink.gene.loc',
    '--gene-map-in', f'{downloaded_files}/gencode.gene.map',
    '--gene-loc-file-huge', f'{downloaded_files}/refGene_hg19_TSS.subset.loc',
    '--exons-loc-file-huge', f'{downloaded_files}/NCBI37.3.plink.gene.exons.loc',
    '--gene-stats-out', 'gs.out',
    '--gene-set-stats-out', 'gss.out',
    '--gene-gene-set-stats-out', 'ggss.out',
    '--gene-effectors-out', 'ge.out'
]

def run_pigean(trait_type, phenotype, gene_set_size):
    cmd = base_cmd + trait_type_command(trait_type) + get_gene_sets(gene_set_size) + get_background_prior(phenotype)
    subprocess.check_call(cmd)


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
    upload('gs.out', file_path)
    upload('gss.out', file_path)
    upload('ggss.out', file_path)
    upload('ge.out', file_path)
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
        os.remove(file_name(args.trait_type))
    except:
        print('ERROR')


if __name__ == '__main__':
    main()


