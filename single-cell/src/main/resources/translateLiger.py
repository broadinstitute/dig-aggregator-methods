import argparse
import glob
import os
import re
import subprocess
import shutil


s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']


def download(dataset):
    path = f'{s3_in}/out/single_cell/staging/liger/{dataset}/liger.zip'
    cmd = ['aws', 's3', 'cp', path, 'inputs/']
    subprocess.check_call(cmd)
    cmd = ['unzip', 'inputs/liger.zip', '-d', 'inputs/']
    subprocess.check_call(cmd)


def format_cell_type(cell_type):
    return re.sub(r'[^a-zA-Z0-9_-]', '', cell_type.replace(' ', '_').lower())


def get_cell_map():
    files = glob.glob('inputs/output/*/metadata.txt')
    cell_types = [re.findall('inputs/output/(.*)/metadata.txt', file)[0] for file in files]
    print(cell_types)
    return {format_cell_type(cell_type): cell_type for cell_type in cell_types}


def convert_cell_loadings(cell_type, cell_type_name):
    with open(f'outputs/{cell_type}/factor_matrix_cell_loadings.tsv', 'w') as f_out:
        with open(f'inputs/output/{cell_type_name}/cell_scores.tsv', 'r') as f_in:
            header = f_in.readline()
            f_out.write('cell\tindep\t{}'.format(header))
            for line in f_in:
                batch_plus_cell, data = line.split('\t', 1)
                batch, cell = batch_plus_cell.split('_', 1)  # Will have to actually do right somehow?
                f_out.write('{}\tTrue\t{}'.format(cell, data))


def convert_gene_loadings(cell_type, cell_type_name):
    with open(f'outputs/{cell_type}/factor_matrix_gene_loadings.tsv', 'w') as f_out:
        with open(f'inputs/output/{cell_type_name}/gene_loadings.tsv', 'r') as f_in:
            header = f_in.readline()
            f_out.write('gene\t{}'.format(header))
            for line in f_in:
                f_out.write(line)


def convert_gene_probabilities(cell_type, cell_type_name):
    pass


def get_top_genes(cell_type):
    with open(f'outputs/{cell_type}/factor_matrix_gene_loadings.tsv', 'r') as f_in:
        header = f_in.readline().strip().split('\t')[1:]
        top_genes = {factor: [] for factor in header}
        for line in f_in:
            gene, data = line.strip().split('\t', 1)
            factor_data = list(map(float, data.split('\t')))
            for factor, factor_datum in zip(header, factor_data):
                top_genes[factor].append((factor_datum, gene))
    return {factor: [a[1] for a in sorted(top_genes[factor], reverse=True)[:5]] for factor in top_genes}


def get_top_cells(cell_type):
    with open(f'outputs/{cell_type}/factor_matrix_cell_loadings.tsv', 'r') as f_in:
        header = f_in.readline().strip().split('\t')[2:]
        top_cells = {factor: [] for factor in header}
        for line in f_in:
            cell, indep, data = line.strip().split('\t', 2)
            factor_data = list(map(float, data.split('\t')))
            for factor, factor_datum in zip(header, factor_data):
                top_cells[factor].append((factor_datum, cell))
    return {factor: [a[1] for a in sorted(top_cells[factor], reverse=True)[:5]] for factor in top_cells}


def convert_gene_programs(cell_type, cell_type_name):
    top_genes = get_top_genes(cell_type)
    top_cells = get_top_cells(cell_type)
    with open(f'inputs/output/{cell_type_name}/gene_programs.txt', 'r') as f:
        factors = f.readline().strip().split('\t')
    with open(f'outputs/{cell_type}/factor_matrix_factors.tsv', 'w') as f_out:
        f_out.write('factor_index\texp_lambdak\ttop_genes\ttop_cells\n')
        for factor in factors:
            f_out.write('{}\t1.0\t{}\t{}\n'.format(
                re.findall(r'Factor_([0-9]*)', factor)[0],
                ','.join(top_genes[factor]),
                ','.join(top_cells[factor])
            ))


def convert(cell_type, cell_type_name):
    os.makedirs(f'outputs/{cell_type}/', exist_ok=True)
    convert_cell_loadings(cell_type, cell_type_name)
    convert_gene_loadings(cell_type, cell_type_name)
    convert_gene_probabilities(cell_type, cell_type_name)
    convert_gene_programs(cell_type, cell_type_name)


def upload(dataset, cell_type, model):
    path = f'{s3_out}/out/single_cell/staging/factor_matrix/{dataset}/{cell_type}/{model}'
    cmd = ['aws', 's3', 'cp', f'outputs/{cell_type}/', path, '--recursive']
    subprocess.check_call(cmd)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset', default=None, required=True, type=str,
                        help="Dataset name")
    parser.add_argument('--model', default=None, required=True, type=str,
                        help="Model")
    args = parser.parse_args()

    download(args.dataset)
    cell_map = get_cell_map()
    for cell_type, cell_type_name in cell_map.items():
        convert(cell_type, cell_type_name)
        upload(args.dataset, cell_type, args.model)
    # shutil.rmtree('inputs')
    # shutil.rmtree('outputs')


if __name__ == '__main__':
    main()
