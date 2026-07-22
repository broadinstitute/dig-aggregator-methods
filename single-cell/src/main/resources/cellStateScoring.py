#!/usr/bin/python3
import argparse
import gzip
import os
import shutil
import subprocess

import pandas as pd

downloaded_files = '/mnt/var/single_cell'
s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']

dataset_to_tissue = {
    'FNIH_Kidney_scRNA_v2.2': 'kidney'
}


def download_data(dataset, cell_type):
    subprocess.check_call(['aws', 's3', 'cp', f'{s3_in}/out/single_cell/staging/downsample/{dataset}/{cell_type}/norm_counts.tsv.gz', 'inputs/'])
    subprocess.check_call(['aws', 's3', 'cp', f'{s3_in}/out/single_cell/staging/downsample/{dataset}/{cell_type}/norm_counts.metadata.tsv.gz', 'inputs/'])
    subprocess.check_call(['aws', 's3', 'cp', f'{s3_in}/out/single_cell/staging/liger/{dataset}/{cell_type}/gene_loadings.tsv', 'inputs/'])


def prepare_sparse_matrix():
    cmd = [
        'python3.11', f'{downloaded_files}/dig-cell-state-scoring/scripts/convert_expression_tsv_to_sparse_10x.py',
        '--matrix-tsv', 'inputs/norm_counts.tsv.gz',
        '--out-dir', 'outputs/rank_10x',
        '--orientation', 'gene_by_cell',
        '--value-type', 'raw_counts'
    ]
    subprocess.check_call(cmd)


metadata_fields = ['cell_id', 'map_id', 'tissue', 'cell_type', 'annotated_cell_type', 'donor_id', 'sample_id']
def prepare_metadata(cell_type, tissue):
    cells = []
    with gzip.open('outputs/rank_10x/barcodes.tsv.gz', 'rt') as f:
        for line in f:
            cells.append(line.strip())
    with gzip.open('outputs/metadata.tsv.gz', 'wt') as f_out:
        f_out.write('{}\n'.format('\t'.join(metadata_fields)))
        with gzip.open('inputs/norm_counts.metadata.tsv.gz', 'rt') as f:
            header = f.readline().strip().split('\t')
            for line in f:
                out_line = {}
                dict_line = dict(zip(header, line.strip().split('\t')))
                if dict_line['ID'] in cells:
                    out_line['cell_id'] = dict_line['ID']
                    out_line['map_id'] = tissue
                    out_line['tissue'] = tissue
                    out_line['cell_type'] = cell_type
                    out_line['annotated_cell_type'] = cell_type
                    out_line['donor_id'] = dict_line['donor_id']
                    out_line['sample_id'] = dict_line['donor_id']
                    f_out.write('{}\n'.format(
                        '\t'.join([str(out_line[k]) for k in metadata_fields])
                    ))

    with gzip.open('outputs/minimal_expression.tsv.gz', 'wt') as f:
        f.write('cell_id\tgene\texpression\n')
        for cell in cells:
            f.write(f'{cell}\tDUMMY_GENE\t0.0\n')


def filter_cell_stats(tissue, cell_type):
    state_ids = set()
    curated_manifest_rows = []
    with open(f'{downloaded_files}/misc/curated_cell_state_manifest.tsv', 'r') as f:
        header = f.readline().strip().split('\t')
        for line in f:
            dict_line = dict(zip(header, line.strip().split('\t')))
            if dict_line['tissue_id'] == tissue and dict_line['cell_type_id'] == cell_type:
                state_ids |= {dict_line['state_id']}
                curated_manifest_rows.append(
                    {
                        'state_name': dict_line['state_id'],
                        'tissue': dict_line['tissue_id'],
                        'cell_type': dict_line['cell_type_id'],
                        'state_class': dict_line.get('state_class', 'unknown'),
                        'is_composite_required': str(dict_line.get('is_composite_required', 'false')).lower(),
                        'signature_kind': 'curated_state',
                    }
                )

    curated_rows = []
    with open(f'{downloaded_files}/misc/{tissue}_cell_state_markers.gmt', 'r') as f:
        for line in f:
            split_line = line.strip().split('\t')
            if split_line[0] in state_ids:
                curated_rows.append((split_line[0], split_line[1], [g for g in split_line[2:] if g]))

    with open('outputs/combined_gmt/curated_state.gmt', 'w') as f:
        for row in curated_rows:
            f.write('{}\t{}\t{}\n'.format(
                row[0],
                row[1],
                '\t'.join(row[2])
            ))

    return curated_manifest_rows, curated_rows


def convert_program_loadings(dataset, tissue, cell_type):
    program_rows = []
    program_manifest_rows = []
    loadings_path = 'inputs/gene_loadings.tsv'

    loadings = pd.read_csv(loadings_path, sep='\t', index_col=0)
    renamed = {}
    for factor in loadings.columns:
        factor_id = factor.replace('Factor_', 'factor_')
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
    loadings.rename(columns=renamed)\
        .reset_index(names='gene')\
        .to_csv('outputs/combined_gmt/program_loadings.tsv.gz', sep='\t', index=False, compression='gzip')


    with open('outputs/combined_gmt/program.gmt', 'w') as f:
        for row in program_rows:
            f.write('{}\t{}\t{}\n'.format(
                row[0],
                row[1],
                '\t'.join(row[2])
            ))

    return renamed, program_manifest_rows, program_rows


def build_combined_gmt(dataset, tissue, cell_type):
    os.makedirs('outputs/combined_gmt', exist_ok=True)
    curated_manifest_rows, curated_rows = filter_cell_stats(tissue, cell_type)
    renamed, program_manifest_rows, program_rows = convert_program_loadings(dataset, tissue, cell_type)

    combined_rows = curated_rows + program_rows
    with open('outputs/combined_gmt/combined_signatures.gmt', 'w') as f:
        for row in combined_rows:
            f.write('{}\t{}\t{}\n'.format(
                row[0],
                row[1],
                '\t'.join(row[2])
            ))

    with open('outputs/combined_gmt/combined_signature_manifest.tsv', 'w') as f:
        f.write('state_name\ttissue\tcell_type\tstate_class\tis_composite_required\tsignature_kind\n')
        for row in curated_manifest_rows + program_manifest_rows:
            f.write('{}\t{}\t{}\t{}\t{}\t{}\n'.format(
                row['state_name'],
                row['tissue'],
                row['cell_type'],
                row['state_class'],
                row['is_composite_required'],
                row['signature_kind'])
            )

    with open('outputs/combined_gmt/signature_kind.tsv', 'w') as f:
        f.write('state_name\tcell_type\tsignature_kind\n')
        for row in curated_manifest_rows + program_manifest_rows:
            f.write('{}\t{}\t{}\n'.format(
                row['state_name'],
                row['cell_type'],
                row['signature_kind'])
            )

    with open('outputs/program_source.manifest.tsv', 'w') as f:
        f.write('cell_type\tprogram_dir\tprogram_loadings\tprogram_cell_activity\n')
        f.write('{}\t{}\t{}\t{}\n'.format(
            cell_type,
            'inputs',
            'outputs/combined_gmt/program_loadings.tsv.gz',
            'outputs/scoring/program_cell_activity.tsv.gz',
        ))


def run_scoring():
    cmd = [
        'python3.11', f'{downloaded_files}/dig-cell-state-scoring/scripts/run_cmdkp_state_scoring.py',
        '--rank-10x-dir', 'outputs/rank_10x',
        '--rank-value-type', 'raw_counts',
        '--expression-matrix', 'outputs/minimal_expression.tsv.gz',
        '--expression-kind', 'linear_normalized',
        '--cell-metadata', 'outputs/metadata.tsv.gz',
        '--states-gmt', 'outputs/combined_gmt/combined_signatures.gmt',
        '--state-manifest', 'outputs/combined_gmt/combined_signature_manifest.tsv',
        '--require-state-manifest',
        '--qc-gmt', f'{downloaded_files}/misc/cmdkp_all_tissues_minimal_bad_cell_qc_signatures.gmt',
        '--allow-small-rank-universe',
        '--map-id-col', 'map_id',
        '--tissue-col', 'tissue',
        '--cell-type-col', 'annotated_cell_type',
        '--donor-col', 'donor_id',
        '--sample-col', 'sample_id',
        '--progress-every-cells', '10000',
        '--legacy-selected-gene-summaries', 'skip',
        '--api-minimal-output',
        '--allow-acceptance-failures',
        '--out-dir', 'outputs/scoring',
    ]
    subprocess.check_call(cmd)


def run_expression_summary():
    cmd = [
        'python3.11', f'{downloaded_files}/dig-cell-state-scoring/scripts/summarize_state_expression.py',
        '--raw-10x-dir', 'outputs/rank_10x',
        '--expression-value-type', 'raw_counts',
        '--metadata', 'outputs/metadata.tsv.gz',
        '--cell-state-activity', 'outputs/scoring/cell_state_activity.tsv.gz',
        '--states-gmt', 'outputs/combined_gmt/combined_signatures.gmt',
        '--parent-group-cols', 'tissue,annotated_cell_type',
        '--cell-type-col', 'annotated_cell_type',
        '--donor-col', 'donor_id',
        '--donor-expression-genes', 'none',
        '--no-write-donor-state-expression',
        '--api-minimal-output',
        '--out-dir', 'outputs/expression',
    ]
    subprocess.check_call(cmd)


def split_by_signature_kind():
    kind = pd.read_csv('outputs/combined_gmt/signature_kind.tsv', sep='\t')
    kind_lookup = kind[['state_name', 'signature_kind']].drop_duplicates()

    expr = pd.read_csv('outputs/expression/all_gene_state_expression_specificity_cp10k.tsv.gz', sep='\t')\
        .merge(kind_lookup, on='state_name', how='left')
    curated_expr = expr[expr['signature_kind'].eq('curated_state')].copy()
    curated_expr.to_csv('outputs/expression/curated_state_expression.tsv.gz', sep='\t', index=False, compression='gzip')
    program_expr = expr[expr['signature_kind'].eq('program')].copy()
    program_expr.to_csv('outputs/expression/program_expression.tsv.gz', sep='\t', index=False, compression='gzip')

    activity = pd.read_csv('outputs/scoring/cell_state_activity.tsv.gz', sep='\t')\
        .merge(kind_lookup, on='state_name', how='left')
    curated_activity = activity[activity['signature_kind'].eq('curated_state')].copy()
    curated_activity.to_csv('outputs/scoring/curated_state_activity.tsv.gz', sep='\t', index=False, compression='gzip')
    curated_activity = activity[activity['signature_kind'].eq('program')].copy()
    curated_activity.to_csv('outputs/scoring/program_activity.tsv.gz', sep='\t', index=False, compression='gzip')



def run_program_state_matching(tissue, cell_type):
    cmd = [
        'python3.11', f'{downloaded_files}/dig-cell-state-scoring/scripts/match_programs_to_cell_states.py',
        '--program-loadings', 'outputs/combined_gmt/program_loadings.tsv.gz',
        '--state-gmt', 'outputs/combined_gmt/curated_state.gmt',
        '--cell-state-activity', 'outputs/scoring/curated_state_activity.tsv.gz',
        '--state-expression', 'outputs/expression/curated_state_expression.tsv.gz',
        '--program-cell-activity', 'outputs/scoring/program_activity.tsv.gz',
        '--metadata', 'outputs/metadata.tsv.gz',
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


def run_pigean(dataset, tissue, kind):
    cmd = [
        'python3.11', f'{downloaded_files}/dig-cell-state-scoring/scripts/run_api_pigean.py',
        '--gmt-dir', 'outputs/combined_gmt',
        '--out-dir', f'outputs/pigean/{kind}',
        '--combined-out', f'outputs/pigean/{kind}/combined_pigean.tsv.gz',
        '--kind', 'curated' if kind == 'curated' else 'program',
        '--tissue', tissue,
        '--dataset', dataset,
        '--model', 'mouse_msigdb',
        '--python', 'python3.11',
        '--pythonpath', f'{downloaded_files}/pigean/pigean/src',
        '--multi-y-in', f'{downloaded_files}/pigean/gs_mouse_msigdb.tsv',
        '--multi-y-id-col', 'gene',
        '--multi-y-pheno-col', 'trait',
        '--multi-y-log-bf-col', 'log_bf',
        '--multi-y-combined-col', 'combined',
        '--multi-y-prior-col', 'huge',
        '--trait-blacklist-in', 'auto',
        '--gene-universe-in', f'{downloaded_files}/pigean/NCBI37.3.plink.gene.loc',
    ]
    subprocess.check_call(cmd)


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


def run_pipeline(dataset, tissue, cell_type):
    os.makedirs('outputs', exist_ok=True)
    prepare_sparse_matrix()
    prepare_metadata(cell_type, tissue)
    build_combined_gmt(dataset, tissue, cell_type)
    run_scoring()
    run_expression_summary()
    split_by_signature_kind()
    run_program_state_matching(tissue, cell_type)
    build_qc_outputs(tissue, cell_type)
    run_pigean(dataset, tissue, 'curated')
    run_pigean(dataset, tissue, 'program')


def upload_data(dataset, cell_type):
    subprocess.check_call(['zip', '-r', 'raw_cell_scoring.zip', 'outputs/'])
    subprocess.check_call(['aws', 's3', 'cp', 'raw_cell_scoring.zip', f'{s3_in}/out/single_cell/staging/scoring/{dataset}/{cell_type}/'])
    os.remove('raw_cell_scoring.zip')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--tissue')  # probably just need to define this somewhere instead
    parser.add_argument('--dataset')
    parser.add_argument('--cell-type')
    args = parser.parse_args()

    tissue = dataset_to_tissue[args.dataset]

    download_data(args.dataset, args.cell_type)
    run_pipeline(args.dataset, tissue, args.cell_type)
    upload_data(args.dataset, args.cell_type)
    shutil.rmtree('outputs')
    shutil.rmtree('inputs')

if __name__ == '__main__':
    main()
