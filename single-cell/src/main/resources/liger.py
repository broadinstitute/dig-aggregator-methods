#!/usr/bin/python3
import argparse
import os
import subprocess

downloaded_files = '/mnt/var/single_cell'
s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']


def download(dataset):
    path = f'{s3_in}/single_cell/{dataset}/data.h5ad'
    cmd = ['aws', 's3', 'cp', path, 'inputs/']
    subprocess.check_call(cmd)


def run_liger():
    cmd = [
        'Rscript',
        f'{downloaded_files}/inmf_liger_mod.R',
        './inputs/data.h5ad',
        './output',
        'donor_id',
        'cell_type__kp',
        '50000'
    ]
    subprocess.check_call(cmd)


def upload(dataset):
    zip_cmd = ['zip', '-r', 'liger.zip', './output/']
    subprocess.check_call(zip_cmd)
    path = f'{s3_out}/out/single_cell/staging/liger/{dataset}/'
    cmd = ['aws', 's3', 'cp', 'liger.zip', path]
    subprocess.check_call(cmd)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset', default=None, required=True, type=str,
                        help="Dataset name")
    args = parser.parse_args()

    download(args.dataset)
    run_liger()
    upload(args.dataset)

# NOTE: I think I do need to create the h5da file again because I need donor_id in there as well

#Result
"""
cell_scores.tsv  gene_loadings.tsv  gene_programs.txt  metadata.txt
[ec2-user@ip-12-0-196-130 endothelial]$ wc -l ./*
  3710 ./cell_scores.tsv
  1001 ./gene_loadings.tsv
    51 ./gene_programs.txt
     2 ./metadata.txt
"""

"""
Factor_1	Factor_2	Factor_3	Factor_4	Factor_5	Factor_6	Factor_7	Factor_8	Factor_9	Factor_10
endothelial_TTCCTTCCAACAAGAT-1-03_SQ	0.000452257074333093	7.9625649725926e-06	0.00110854251065932	0.000365090581565909	0	0.000221838603302801	5.32875211948636e-05	0	0.000248353475643771	0
"""

"""
Factor_1	Factor_2	Factor_3	Factor_4	Factor_5	Factor_6	Factor_7	Factor_8	Factor_9	Factor_10
AADAT	6.05065456798833	0	0	7.3291084220361	62.6838178262246	0	19.3285246749231	0	0.971992006294117	1.00829488670829
"""

"""
cell_type	k	method	timestamp
endothelial	10	LIGER_iNMF	2026-02-26 02:43:40.479543
"""


if __name__ == '__main__':
    main()
