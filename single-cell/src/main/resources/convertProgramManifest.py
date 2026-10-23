#!/usr/bin/python3
import argparse
import os
import subprocess
import shutil
import pandas as pd

s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']


def download_data(tissue, cell_type, dataset):
    path = f'{s3_in}/out/single_cell/staging/nmf/liger/{tissue}/{cell_type}/{dataset}/factor_gene_programs.csv'
    cmd = ['aws', 's3', 'cp', path, 'inputs/']
    subprocess.check_call(cmd)


def convert_program_loadings(tissue, cell_type, dataset):
    program_rows = []
    program_manifest_rows = []
    loadings_path = 'inputs/factor_gene_programs.csv'

    loadings = pd.read_csv(loadings_path, sep=',', index_col=0)
    renamed = {}
    for factor in loadings.columns:
        factor_id = factor.replace('Factor_factor', 'factor_')
        state_name = f'{tissue}_{cell_type}_program_{factor_id}'
        renamed[factor] = state_name
        top = loadings[factor].sort_values(ascending=False).head(100) # take top 100 genes
        genes = [str(g) for g, v in top.items() if pd.notna(v) and float(v) > 0]
        if genes:
            program_rows.append((state_name, f'type=program;cell_type={cell_type};source={dataset}', genes))
            program_manifest_rows.append({
                'state_name': state_name,
                'tissue': tissue,
                'cell_type': cell_type,
                'state_class': 'broad_function_gradient',
                'is_composite_required': 'false',
                'signature_kind': 'program',
            })

    os.makedirs(f'outputs/programs/{tissue}/{cell_type}/{dataset}', exist_ok=True)
    with open(f'outputs/programs/{tissue}/{cell_type}/{dataset}/manifest.tsv', 'w') as f:
        f.write('state_name\ttissue\tcell_type\tstate_class\tis_composite_required\tsignature_kind\n')
        for row in program_manifest_rows:
            f.write('{}\t{}\t{}\t{}\t{}\t{}\n'.format(
                row['state_name'],
                row['tissue'],
                row['cell_type'],
                row['state_class'],
                row['is_composite_required'],
                row['signature_kind'])
            )

    loadings.rename(columns=renamed) \
        .reset_index(names='gene') \
        .to_csv(f'outputs/programs/{tissue}/{cell_type}/{dataset}/program_loadings.tsv.gz', sep='\t', index=False, compression='gzip')

    with open(f'outputs/programs/{tissue}/{cell_type}/{dataset}/gene_sets.gmt', 'w') as f:
        for row in program_rows:
            f.write('{}\t{}\t{}\n'.format(
                row[0],
                row[1],
                '\t'.join(row[2])
            ))

    return renamed, program_manifest_rows, program_rows


def upload_data():
    cmd = ['aws', 's3', 'cp', 'outputs/', f'{s3_out}/out/single_cell/gene_sets/', '--recursive']
    subprocess.check_call(cmd)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--tissue', default=None, required=True, type=str,
                        help="Tissue")
    parser.add_argument('--cell-type', default=None, required=True, type=str,
                        help="Cell Type")
    parser.add_argument('--dataset', default=None, required=True, type=str,
                        help="Dataset name")
    args = parser.parse_args()

    download_data(args.tissue, args.cell_type, args.dataset)
    convert_program_loadings(args.tissue, args.cell_type, args.dataset)
    upload_data()
    shutil.rmtree('inputs')
    shutil.rmtree('outputs')


if __name__ == '__main__':
    main()
