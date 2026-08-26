#!/usr/bin/python3
import argparse
import glob
import os
import shutil
import subprocess

import pandas as pd

downloaded_files = '/mnt/var/single_cell'
s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']

dataset_to_tissue = {
    'islet_of_Langerhans_scRNA_v3-4': 'pancreas'
}


def download_data(dataset, cell_type):
    subprocess.check_call(['aws', 's3', 'cp', f'{s3_in}/out/single_cell/staging/mtx/{dataset}/{cell_type}/', 'inputs/', '--recursive'])
    subprocess.check_call(['aws', 's3', 'cp', f'{s3_in}/out/single_cell/staging/liger/{dataset}/{cell_type}/gene_loadings.tsv', 'inputs/'])
    subprocess.check_call(['aws', 's3', 'cp', f'{s3_in}/out/single_cell/gene_sets/', 'gene_sets/', '--recursive'])


def build_combined(tissue, cell_type):
    os.makedirs('combined', exist_ok=True)

    with open('combined/gene_sets.gmt', 'w') as f:
        header = None
        for file in glob.glob(f'gene_sets/*/{tissue}/{cell_type}/*/gene_sets.gmt'):
            with open(file, 'r') as f_in:
                if header is None:
                    header = f_in.readline()
                    f.write(header)
                else:
                    _ = f_in.readline()
                for line in f_in:
                    f.write(line)

    with open('combined/manifest.tsv', 'w') as f:
        header = None
        for file in glob.glob(f'gene_sets/*/{tissue}/{cell_type}/*/manifest.tsv'):
            with open(file, 'r') as f_in:
                if header is None:
                    header = f_in.readline()
                    f.write(header)
                else:
                    _ = f_in.readline()
                for line in f_in:
                    f.write(line)


def run_scoring():
    cmd = [
        'python3.11', f'{downloaded_files}/dig-cell-state-scoring/scripts/run_cmdkp_state_scoring.py',
        '--rank-10x-dir', 'inputs',
        '--rank-value-type', 'log1p_cp10k',
        '--cell-metadata', 'inputs/metadata.tsv.gz',
        '--states-gmt', 'combined/gene_sets.gmt',
        '--state-manifest', 'combined/manifest.tsv',
        '--require-state-manifest',
        '--qc-gmt', f'{downloaded_files}/misc/cmdkp_all_tissues_minimal_bad_cell_qc_signatures.gmt',
        '--allow-small-rank-universe',
        '--tissue-col', 'tissue',
        '--cell-type-col', 'cell_type',
        '--donor-col', 'donor_id',
        '--sample-col', 'sample_id',
        '--progress-every-cells', '10000',
        '--legacy-selected-gene-summaries', 'skip',
        '--api-minimal-output',
        '--out-dir', 'outputs/scoring',
    ]
    subprocess.check_call(cmd)


def run_expression_summary():
    cmd = [
        'python3.11', f'{downloaded_files}/dig-cell-state-scoring/scripts/summarize_state_expression.py',
        '--raw-10x-dir', 'inputs',
        '--expression-value-type', 'log1p_cp10k',
        '--metadata', 'inputs/metadata.tsv.gz',
        '--cell-state-activity', 'outputs/scoring/cell_state_activity.tsv.gz',
        '--cell-type-col', 'annotated_cell_type',
        '--api-minimal-output',
        '--out-dir', 'outputs/expression',
    ]
    subprocess.check_call(cmd)


def split_by_signature_kind():
    kind = pd.read_csv('combined/manifest.tsv', sep='\t')
    kind_lookup = kind[['state_name', 'signature_kind']].drop_duplicates()

    expr = pd.read_csv('outputs/expression/all_gene_state_expression_specificity_cp10k.tsv.gz', sep='\t') \
        .merge(kind_lookup, on='state_name', how='left')
    curated_expr = expr[expr['signature_kind'].eq('curated_state')].copy()
    curated_expr.to_csv('outputs/expression/curated_state_expression.tsv.gz', sep='\t', index=False, compression='gzip')
    program_expr = expr[expr['signature_kind'].eq('program')].copy()
    program_expr.to_csv('outputs/expression/program_expression.tsv.gz', sep='\t', index=False, compression='gzip')

    activity = pd.read_csv('outputs/scoring/cell_state_activity.tsv.gz', sep='\t') \
        .merge(kind_lookup, on='state_name', how='left')
    curated_activity = activity[activity['signature_kind'].eq('curated_state')].copy()
    curated_activity.to_csv('outputs/scoring/curated_state_activity.tsv.gz', sep='\t', index=False, compression='gzip')
    program_activity = activity[activity['signature_kind'].eq('program')].copy()
    program_activity = program_activity.rename(columns={'state_name': 'program_id', 'aucell_score': 'program_activity'})
    program_activity.to_csv('outputs/scoring/program_activity.tsv.gz', sep='\t', index=False, compression='gzip')



def run_program_state_matching(tissue, cell_type):
    cmd = [
        'python3.11', f'{downloaded_files}/dig-cell-state-scoring/scripts/match_programs_to_cell_states.py',
        '--program-loadings', 'outputs/combined_gmt/program_loadings.tsv.gz',
        '--state-gmt', 'outputs/combined_gmt/curated_state.gmt',
        '--cell-state-activity', 'outputs/scoring/curated_state_activity.tsv.gz',
        '--program-cell-activity', 'outputs/scoring/program_activity.tsv.gz',
        '--tissue', tissue,
        '--cell-type', cell_type,
        '--gsea-permutations', '1000',
        '--qc-gmt', f'{downloaded_files}/misc/cmdkp_all_tissues_minimal_bad_cell_qc_signatures.gmt',
        '--out-dir', 'outputs/match',
    ]
    subprocess.check_call(cmd)

    summary = pd.read_csv('outputs/match/program_state_match_summary.tsv.gz', sep='\t', compression='infer', low_memory=False)
    if not summary.empty:
        heat = pd.DataFrame({
            'tissue': tissue,
            'cell_type': cell_type,
            'state_id': summary['state_id'],
            'program_id': summary['program_id'],
            'correlation': summary.get('cell_spearman_r_gradient'),
            'gsea_p': summary.get('gsea_p'),
            'gsea_q': summary.get('gsea_q'),
        })
        heat.to_csv('outputs/match/program_state_heatmap_long.tsv.gz', sep='\t', index=False, compression='gzip')


def build_qc_outputs(tissue, cell_type):
    qc_match_frames = []
    qc_enrichment_frames = []
    qc_match = pd.read_csv('outputs/match/program_qc_match_summary.tsv.gz', sep='\t', compression='infer', low_memory=False)
    if not qc_match.empty:
        qc_match.insert(0, 'cell_type', cell_type)
        qc_match.insert(0, 'tissue', tissue)
        qc_match_frames.append(qc_match)
    enrichment = pd.read_csv('outputs/match/program_state_marker_enrichment.tsv.gz', sep='\t', compression='infer', low_memory=False)
    if not enrichment.empty and 'state_type' in enrichment.columns:
        qc_enrichment_frames.append(enrichment[enrichment['state_type'].eq('qc_state')].copy())

    out_qc_match = pd.concat(qc_match_frames, ignore_index=True) if qc_match_frames else pd.DataFrame()
    out_qc_match.to_csv('outputs/match/program_qc_match_summary.tsv.gz', sep='\t', index=False, compression='gzip')
    out_qc_enrichment = pd.concat(qc_enrichment_frames, ignore_index=True) if qc_enrichment_frames else pd.DataFrame()
    out_qc_enrichment.to_csv('outputs/match/program_qc_enrichment.tsv.gz', sep='\t', index=False, compression='gzip')


def run_pipeline(tissue, cell_type):
    os.makedirs('outputs', exist_ok=True)
    build_combined(tissue, cell_type)
    run_scoring()
    run_expression_summary()
    split_by_signature_kind()
    run_program_state_matching(tissue, cell_type)
    build_qc_outputs(tissue, cell_type)


def upload_data(dataset, cell_type):
    subprocess.check_call(['zip', '-r', 'raw_cell_scoring.zip', 'outputs/'])
    subprocess.check_call(['aws', 's3', 'cp', 'raw_cell_scoring.zip', f'{s3_in}/out/single_cell/staging/scoring/{dataset}/{cell_type}/'])
    os.remove('raw_cell_scoring.zip')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset')
    parser.add_argument('--cell-type')
    args = parser.parse_args()

    tissue = dataset_to_tissue[args.dataset]

    download_data(args.dataset, args.cell_type)
    run_pipeline(tissue, args.cell_type)
    upload_data(args.dataset, args.cell_type)
    shutil.rmtree('outputs')
    shutil.rmtree('inputs')

if __name__ == '__main__':
    main()
