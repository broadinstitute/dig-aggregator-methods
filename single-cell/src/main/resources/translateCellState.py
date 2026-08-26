#!/usr/bin/python3
import argparse
import glob
import numpy as np
import os
import shutil
import subprocess
import zipfile

import pandas as pd

downloaded_files = '/mnt/var/single_cell'
s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']

dataset_to_tissue = {
    'islet_of_Langerhans_scRNA_v3-4': 'pancreas'
}


def download_data(dataset):
    cmd = ['aws', 's3', 'cp', f'{s3_in}/out/single_cell/staging/scoring/{dataset}/', 'inputs/', '--recursive']
    subprocess.check_call(cmd)
    cmd = ['aws', 's3', 'cp', f'{s3_in}/out/single_cell/factors/{dataset}/', f'inputs/factors/{dataset}/', '--recursive']
    subprocess.check_call(cmd)


def extract_zips():
    cell_types = []
    for zip_path in glob.glob('inputs/*/raw_cell_scoring.zip'):
        cell_type = os.path.basename(os.path.dirname(zip_path))
        cell_types.append(cell_type)
        with zipfile.ZipFile(zip_path) as z:
            z.extractall(f'work/{cell_type}')
    return cell_types


def concat_tables(cell_types, relative_path):
    frames = []
    for cell_type in cell_types:
        path = f'work/{cell_type}/{relative_path}'
        if os.path.exists(path):
            frames.append(pd.read_csv(path, sep='\t', compression='infer', low_memory=False))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def add_log2fc_weighted_vs_all_parent(frame):
    frame = frame.copy()
    weighted_contribution = frame['weighted_mean_expression'] * frame['n_parent']
    group_weighted_sum = weighted_contribution.groupby(frame['gene']).transform('sum')
    group_n_parent_sum = frame['n_parent'].groupby(frame['gene']).transform('sum')
    other_weighted_sum = group_weighted_sum - weighted_contribution
    other_n_parent = group_n_parent_sum - frame['n_parent']
    other_mean_expression = other_weighted_sum / other_n_parent.where(other_n_parent > 0)
    frame['log2fc_weighted_vs_all_parent'] = np.log2(
        (frame['weighted_mean_expression'] + 0.05) / (other_mean_expression + 0.05)
    )
    return frame


def combine_expression(cell_types):
    os.makedirs('outputs/combined', exist_ok=True)
    concat_tables(cell_types, 'outputs/expression/curated_state_expression.tsv.gz') \
        .to_csv('outputs/combined/curated_state_expression.tsv.gz', sep='\t', index=False, compression='gzip')
    concat_tables(cell_types, 'outputs/expression/program_expression.tsv.gz') \
        .to_csv('outputs/combined/program_expression.tsv.gz', sep='\t', index=False, compression='gzip')
    cell_type_expression = concat_tables(cell_types, 'outputs/expression/all_gene_cell_type_expression_cp10k.tsv.gz')
    add_log2fc_weighted_vs_all_parent(cell_type_expression) \
        .to_csv('outputs/combined/cell_type_expression.tsv.gz', sep='\t', index=False, compression='gzip')


def combine_pigean(cell_types):
    # PIGEAN already ran per cell type inside each raw zip; no need to rerun it here.
    concat_tables(cell_types, 'outputs/pigean/curated/combined_pigean.tsv.gz') \
        .to_csv('outputs/combined/cell_state_pigean.tsv.gz', sep='\t', index=False, compression='gzip')
    concat_tables(cell_types, 'outputs/pigean/program/combined_pigean.tsv.gz') \
        .to_csv('outputs/combined/program_pigean.tsv.gz', sep='\t', index=False, compression='gzip')


def build_program_source_manifest(cell_types):
    with open('outputs/combined/program_source_manifest.tsv', 'w') as f:
        f.write('cell_type\tprogram_dir\tprogram_loadings\n')
        for cell_type in cell_types:
            f.write(f'{cell_type}\t{cell_type}\twork/{cell_type}/outputs/combined_gmt/program_loadings.tsv.gz\n')


def build_program_match_dir(cell_types):
    for cell_type in cell_types:
        match_dir = f'outputs/combined/program_state_matches/{cell_type}'
        os.makedirs(match_dir, exist_ok=True)
        for name in ['program_state_heatmap_long.tsv.gz', 'program_label_suggestions.tsv.gz']:
            src = f'work/{cell_type}/outputs/match/{name}'
            if os.path.exists(src):
                shutil.copy2(src, f'{match_dir}/{name}')


def build_portal_tables(dataset):
    cmd = [
        'python', f'{downloaded_files}/dig-cell-state-scoring/scripts/build_portal_api_data_tables.py',
        '--out-dir', 'outputs/portal',
        '--tissue', dataset_to_tissue[dataset],
        '--dataset', dataset,
        '--model', 'mouse_msigdb',
        '--cell-state-expression', 'outputs/combined/curated_state_expression.tsv.gz',
        '--program-expression', 'outputs/combined/program_expression.tsv.gz',
        '--cell-type-expression', 'outputs/combined/cell_type_expression.tsv.gz',
        '--program-loadings-manifest', 'outputs/combined/program_source_manifest.tsv',
        '--program-match-dir', 'outputs/combined/program_state_matches',
        '--cell-state-pigean', 'outputs/combined/cell_state_pigean.tsv.gz',
        '--program-pigean', 'outputs/combined/program_pigean.tsv.gz',
        '--program-factors', f'inputs/factors/{dataset}'
    ]
    subprocess.check_call(cmd)


def build_qc_outputs(cell_types):
    concat_tables(cell_types, 'outputs/match/program_qc_match_summary.tsv.gz') \
        .to_csv('outputs/portal/program_qc_match_summary.tsv.gz', sep='\t', index=False, compression='gzip')
    concat_tables(cell_types, 'outputs/match/program_qc_enrichment.tsv.gz') \
        .to_csv('outputs/portal/program_qc_enrichment.tsv.gz', sep='\t', index=False, compression='gzip')


def run_pipeline(dataset):
    os.makedirs('outputs', exist_ok=True)
    cell_types = extract_zips()
    combine_expression(cell_types)
    combine_pigean(cell_types)
    build_program_source_manifest(cell_types)
    build_program_match_dir(cell_types)
    build_portal_tables(dataset)
    build_qc_outputs(cell_types)


def upload_data(dataset):
    subprocess.check_call(['zip', '-j', '-r', 'portal.zip', 'outputs/portal'])
    subprocess.check_call(['aws', 's3', 'cp', 'portal.zip', f'{s3_out}/out/single_cell/portal/{dataset}/'])
    os.remove('portal.zip')


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument('--dataset')
    args = ap.parse_args()

    download_data(args.dataset)
    run_pipeline(args.dataset)
    upload_data(args.dataset)
    shutil.rmtree('outputs')
    shutil.rmtree('work')


if __name__ == '__main__':
    main()
