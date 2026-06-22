#!/usr/bin/python3
import argparse
import os
import shutil
import subprocess

downloaded_files = '/mnt/var/pigean'
s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']

dataset_to_tissue = {
    'FNIH_Kidney_scRNA_v2.2': 'kidney'
}

def run_dataset(dataset):
    cmd = [
        'PYTHON_CMD=python3.11',
        'TISSUE_ROOT=tissue_data',
        f'TISSUE_ID={dataset_to_tissue[dataset]}',
        'EXPRESSION_TSV=inputs/norm_counts.tsv.gz',
        'EXPRESSION_VALUE_TYPE=auto',
        'METADATA=inputs/sample_metadata.tsv.gz',
        f'STATES_GMT={downloaded_files}/dig-cell-state-scoring/dat/${dataset_to_tissue[dataset]}/${dataset_to_tissue[dataset]}_cell_state_markers.gmt',
        f'STATE_MANIFEST={downloaded_files}/dig-cell-state-scoring/dat/api/curated_cell_state_manifest.tsv',
        f'QC_GMT={downloaded_files}/dig-cell-state-scoring/dat/qc/cmdkp_all_tissues_minimal_bad_cell_qc_signatures.gmt',
        f'PIGEAN_PYTHONPATH={downloaded_files}/pigean/src',
        f'PIGEAN_MULTI_Y_IN={downloaded_files}/gs_mouse_msigdb.tsv',
        f'PIGEAN_GENE_UNIVERSE={downloaded_files}/NCBI37.3.plink.gene.loc',
        f'{downloaded_files}/dig-cell-state-scoring/scripts/run_tissue_api_data_pipeline.sh'
    ]
    subprocess.check_call(cmd)

# Steps
# 1. Downsample (5000 total, or max(5000) per cell_type) - output h5ad file for Liger and the intermediate mtx files for scoring, also normalized metadata file
# 2. (bin) Make state and qc file for use in the scoring
# 3a. Run scoring (python file) (what is the point of minimal_expression.tsv.gz?)
# 3b. Summarize state expression will be part of the the scoring step and splitting as well
# 3c. Heatmap
# 3d. Match cell state to program
# 4. Pigean (for both curated cell states and programs (This I think we just modify what we got, so the key is getting the state scoring and running the bin gmt and program gmt through it)
# 5. Translation


def main():
    opts = argparse.ArgumentParser()
    opts.add_argument('--dataset', type=str, required=True)
    args = opts.parse_args()

    import time
    time.sleep(10*3600)


if __name__ == '__main__':
    main()
