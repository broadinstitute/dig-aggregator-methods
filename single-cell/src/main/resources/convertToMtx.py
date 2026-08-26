#!/usr/bin/python3
import argparse
import gzip
import os
import shutil
import subprocess

downloaded_files = '/mnt/var/single_cell'
s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']

dataset_to_tissue = {
    'islet_of_Langerhans_scRNA_v3-4': 'pancreas'
}


def download_data(dataset, cell_type):
    subprocess.check_call(['aws', 's3', 'cp', f'{s3_in}/out/single_cell/staging/split/{dataset}/{cell_type}/norm_counts.tsv.gz', 'inputs/'])
    subprocess.check_call(['aws', 's3', 'cp', f'{s3_in}/out/single_cell/staging/split/{dataset}/{cell_type}/norm_counts.metadata.tsv.gz', 'inputs/'])


def prepare_sparse_matrix():
    cmd = [
        'python3.11', f'{downloaded_files}/dig-cell-state-scoring/scripts/convert_expression_tsv_to_sparse_10x.py',
        '--matrix-tsv', 'inputs/norm_counts.tsv.gz',
        '--out-dir', 'outputs/rank_10x',
        '--orientation', 'gene_by_cell',
        '--value-type', 'log1p_cp10k'
    ]
    subprocess.check_call(cmd)


metadata_fields = ['cell_id', 'tissue', 'cell_type', 'donor_id', 'sample_id']
def prepare_metadata(cell_type, tissue):
    cells = []
    with gzip.open('outputs/barcodes.tsv.gz', 'rt') as f:
        for line in f:
            cells.append(line.strip())
    with gzip.open('outputs/metadata.tsv.gz', 'wt') as f_out:
        f_out.write('{}\n'.format('\t'.join(metadata_fields)))
        with gzip.open('inputs/norm_counts.metadata.tsv.gz', 'rt') as f:
            header = f.readline().strip().split('\t')
            for line in f:
                out_line = {}
                dict_line = dict(zip(header, line.strip().split('\t')))
                if dict_line['NAME'] in cells:
                    out_line['cell_id'] = dict_line['NAME']
                    out_line['tissue'] = tissue
                    out_line['cell_type'] = cell_type
                    out_line['donor_id'] = dict_line['donor_accession']
                    out_line['sample_id'] = dict_line['barcodes']
                    f_out.write('{}\n'.format(
                        '\t'.join([str(out_line[k]) for k in metadata_fields])
                    ))


def upload_data(dataset, cell_type):
    subprocess.check_call(['aws', 's3', 'cp', 'outputs/rank_10x', f'{s3_in}/out/single_cell/staging/mtx/{dataset}/{cell_type}/', '--recursive'])
    subprocess.check_call(['aws', 's3', 'cp', 'outputs/metadata.tsv.gz', f'{s3_in}/out/single_cell/staging/mtx/{dataset}/{cell_type}/'])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset')
    parser.add_argument('--cell-type')
    args = parser.parse_args()

    download_data(args.dataset, args.cell_type)
    prepare_sparse_matrix()
    prepare_metadata(args.cell_type, dataset_to_tissue[args.dataset])
    upload_data(args.dataset, args.cell_type)
    shutil.rmtree('outputs')
    shutil.rmtree('inputs')


if __name__ == '__main__':
    main()
