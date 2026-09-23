#!/usr/bin/python3
import glob
import gzip
import json
import math
import os
import random
import re
import shutil
import subprocess

import numpy as np

s3_in = os.environ['INPUT_PATH']
s3_bioindex = os.environ['BIOINDEX_PATH']


prod_datasets = {
    'liver': 'FNIH_Liver_scRNA_v4.0'
}


def list_tissues():
    out = subprocess.check_output(['aws', 's3', 'ls', f'{s3_in}/out/single_cell/portal/']).decode()
    return sorted(line.split()[-1].rstrip('/') for line in out.splitlines() if line.strip().endswith('/'))


def download_portal_data(tissues):
    for tissue in tissues:
        subprocess.check_call(['aws', 's3', 'cp', f'{s3_in}/out/single_cell/portal/{tissue}/{prod_datasets[tissue]}/portal.zip', f'inputs/portal/{tissue}/portal.zip'])
        subprocess.check_call(['unzip', '-o', '-q', f'inputs/portal/{tissue}/portal.zip', '-d', f'inputs/portal/{tissue}'])


def download_metadata():
    cmd = ['aws', 's3', 'cp', 's3://dig-analysis-bin/single_cell/scoring/', 'inputs/misc/', '--recursive']
    subprocess.check_call(cmd)


def download_pigean_gene_set():
    path = f'{s3_in}/out/single_cell/pigean/'
    subprocess.check_call(['aws', 's3', 'cp', path, 'inputs/pigean/', '--recursive'])


def part_path(subdir, num=0, hash_=None):
    os.makedirs(f'outputs/gene_program/{subdir}', exist_ok=True)
    hash_ = random.getrandbits(128) if hash_ is None else hash_
    return f'outputs/gene_program/{subdir}/part-{str(num).zfill(5)}-{hex(hash_)[2:-1]}.json'


def build_program_labels(tissues):
    labels = {}
    for tissue in tissues:
        with gzip.open(f'inputs/portal/{tissue}/program_factor_metadata.tsv.gz', 'rt') as f:
            header = f.readline().strip().split('\t')
            for line in f:
                dict_line = dict(zip(header, line.strip().split('\t')))
                key = (tissue, dict_line['cell_type'], dict_line['factor'])
                labels[key] = (dict_line['label'], dict_line['quality'])
    return labels


def build_cell_state_metadata(tissues):
    lines = []
    for tissue in tissues:
        with open(f'inputs/misc/{tissue}_index.json', 'r') as f:
            data = json.load(f)

        for tissue_data in data['tissues']:
            tissue = tissue_data['tissue_id']
            tissue_label = tissue_data['tissue_label']
            for cell_type_data in tissue_data['cell_types']:
                cell_type = cell_type_data['cell_type_id']
                cell_type_label = cell_type_data['cell_type_label']
                for cell_state in cell_type_data['states']:
                    cell_state['tissue'] = tissue
                    cell_state['tissue_label'] = tissue_label
                    cell_state['cell_type'] = cell_type
                    cell_state['cell_type_label'] = cell_type_label
                    lines.append(cell_state)

    with open(part_path('metadata/cell_state'), 'w') as f_out:
        for line in sorted(lines, key=lambda x: (x['tissue'], x['cell_type'], x['state_id'])):
            f_out.write('{}\n'.format(json.dumps(line)))


def build_cell_state_metadata_extended(tissues):
    lines = []
    for tissue in tissues:
        with open(f'inputs/misc/{tissue}_details_by_id.json', 'r') as f:
            data = json.load(f)

        for cell_state_data in data.values():
            cell_state_data['tissue_label'] = cell_state_data['tissue']['label']
            cell_state_data['tissue'] = cell_state_data['tissue']['id']
            cell_state_data['cell_type_label'] = cell_state_data['cell_type']['label']
            cell_state_data['cell_type'] = cell_state_data['cell_type']['id']
            lines.append(cell_state_data)

    with open(part_path('metadata/cell_state_extended'), 'w') as f_out:
        for line in sorted(lines, key=lambda x: (x['tissue'], x['cell_type'])):
            f_out.write('{}\n'.format(json.dumps(line)))


def build_qc_metadata():
    with open('inputs/misc/qc_state_index.json', 'r') as f:
        data = json.load(f)

    lines = []
    for qc_data in data['qc_signatures']:
        qc_data['dummy'] = 1
        lines.append(qc_data)

    with open(part_path('metadata/qc'), 'w') as f_out:
        for line in lines:
            f_out.write('{}\n'.format(json.dumps(line)))


def build_qc_metadata_extended():
    with open('inputs/misc/qc_state_details_by_id.json', 'r') as f:
        data = json.load(f)

    lines = []
    for qc_data in data.values():
        qc_data['dummy'] = 1
        lines.append(qc_data)

    with open(part_path('metadata/qc_extended'), 'w') as f_out:
        for line in lines:
            f_out.write('{}\n'.format(json.dumps(line)))


def build_cell_state_expression_by_tissue(tissues):
    hash_ = random.getrandbits(128)
    for idx, tissue in enumerate(tissues):
        lines = []
        with gzip.open(f'inputs/portal/{tissue}/cell_state_expression.tsv.gz', 'rt') as f:
            header = f.readline().strip().split('\t')
            for line in f:
                dict_line = dict(zip(header, line.strip().split('\t')))
                log10_cpk = float(dict_line['log10_cpk'])
                if log10_cpk > 0.0:
                    log2fc = float(dict_line['log2fc_weighted_vs_all_parent']) if dict_line['log2fc_weighted_vs_all_parent'] != '' else None
                    p_value = float(dict_line['p_value']) if dict_line['p_value'] != '' else None
                    lines.append(
                        {
                            'gene': dict_line['gene'],
                            'dataset': prod_datasets[tissue],
                            'tissue': tissue,
                            'cell_type': dict_line['cell_type'],
                            'state_name': dict_line['state_name'],
                            'log10_cpk': math.log10(log10_cpk),
                            'log2fc_weighted_vs_all_parent': log2fc,
                            'p_value': np.nextafter(0, 1) if p_value == 0 else p_value
                        }
                    )
        with open(part_path('expression/cell_state', num=idx, hash_=hash_), 'w') as f_out:
            for line in sorted(lines, key=lambda x: (x['tissue'], x['cell_type'], x['gene'], x['p_value'])):
                f_out.write('{}\n'.format(json.dumps(line)))


def build_cell_state_pigean(tissues):
    lines = []
    for tissue in tissues:
        with gzip.open(f'inputs/portal/{tissue}/cell_state_pigean_trait_results.tsv.gz', 'rt') as f:
            header = f.readline().strip().split('\t')
            for line in f:
                dict_line = dict(zip(header, line.strip().split('\t')))
                lines.append(
                    {
                        'state_name': dict_line['state_name'],
                        'dataset': prod_datasets[tissue],
                        'tissue': tissue,
                        'cell_type': dict_line['cell_type'],
                        'trait': dict_line['trait'],
                        'beta': float(dict_line['beta']),
                        'beta_uncorrected': float(dict_line['beta_uncorrected'])
                    }
                )

    # No model?
    with open(part_path('factors/cell_state/trait'), 'w') as f_out:
        for line in sorted(lines, key=lambda x: (x['tissue'], x['cell_type'], x['state_name'], -x['beta'])):
            f_out.write('{}\n'.format(json.dumps(line)))


def build_cell_state_heatmap(tissues, labels):
    lines = []
    for tissue in tissues:
        with gzip.open(f'inputs/portal/{tissue}/program_state_heatmap.tsv.gz', 'rt') as f:
            header = f.readline().strip().split('\t')
            for line in f:
                dict_line = dict(zip(header, line.strip().split('\t')))
                modified_factor = 'Factor{}'.format(re.findall(r'factor_([0-9]*)', dict_line['program_id'])[0])
                key = (tissue, dict_line['cell_type'], modified_factor)
                label, quality = labels.get(key, (None, None))
                lines.append(
                    {
                        'state_name': dict_line['state_name'],
                        'program_id': modified_factor,
                        'program_label': label,
                        'program_quality': quality,
                        'dataset': prod_datasets[tissue],
                        'tissue': tissue,
                        'cell_type': dict_line['cell_type'],
                        #'correlation': float(dict_line['correlation']), need to look at this
                        'gsea_p': float(dict_line['gsea_p']) if dict_line.get('gsea_p', '') != '' else None,
                        'gsea_q': float(dict_line['gsea_q']) if dict_line.get('gsea_q', '') != '' else None
                    }
                )

    with open(part_path('heatmap'), 'w') as f_out:
        for line in lines:
            f_out.write('{}\n'.format(json.dumps(line)))


def build_program_factor_metadata(tissues):
    lines = {}
    for tissue in tissues:
        with gzip.open(f'inputs/portal/{tissue}/program_factor_metadata.tsv.gz', 'rt') as f:
            header = f.readline().strip().split('\t')
            for line in f:
                dict_line = dict(zip(header, line.strip().split('\t')))
                key = (tissue, dict_line['cell_type'], dict_line['factor'])
                lines[key] = {
                    'dataset': prod_datasets[tissue],
                    'tissue': tissue,
                    'model': dict_line['model'],
                    'cell_type': dict_line['cell_type'],
                    'factor': dict_line['factor'],
                    'top_genes': dict_line['top_genes'],
                    'label': dict_line['label']
                }  # missing rationale?

    with open(part_path('factors/program/factor'), 'w') as f_out:
        for key in sorted(lines):
            f_out.write('{}\n'.format(json.dumps(lines[key])))


def build_program_pigean(tissues, labels):
    lines = []
    for tissue in tissues:
        with gzip.open(f'inputs/portal/{tissue}/program_pigean_trait_results.tsv.gz', 'rt') as f:
            header = f.readline().strip().split('\t')
            for line in f:
                dict_line = dict(zip(header, line.strip().split('\t')))
                key = (tissue, dict_line['cell_type'], dict_line['factor'])
                label, quality = labels.get(key, (None, None))
                lines.append(
                    {
                        'dataset': prod_datasets[tissue],
                        'tissue': tissue,
                        'model': dict_line['model'],
                        'cell_type': dict_line['cell_type'],
                        'factor': dict_line['factor'],
                        'factor_label': label,
                        'factor_quality': quality,
                        'trait': dict_line['trait'],
                        'beta': float(dict_line['beta']),
                        'beta_uncorrected': float(dict_line['beta_uncorrected'])
                    }
                )

    with open(part_path('factors/program/trait'), 'w') as f_out:
        for line in sorted(lines, key=lambda x: (x['tissue'], x['cell_type'], x['factor'], -x['beta'])):
            f_out.write('{}\n'.format(json.dumps(line)))


def build_program_expression_by_dataset(tissues, labels):
    hash_ = random.getrandbits(128)
    for idx, tissue in enumerate(tissues):
        lines = []
        with gzip.open(f'inputs/portal/{tissue}/program_expression.tsv.gz', 'rt') as f:
            header = f.readline().strip().split('\t')
            for line in f:
                dict_line = dict(zip(header, line.strip().split('\t')))
                log10_cpk = float(dict_line['log10_cpk'])
                if log10_cpk > 0.0:
                    log2fc = float(dict_line['log2fc_weighted_vs_all_parent']) if dict_line['log2fc_weighted_vs_all_parent'] != '' else None
                    p_value = float(dict_line['p_value']) if dict_line['p_value'] != '' else None
                    key = (tissue, dict_line['cell_type'], dict_line['factor'])
                    label, quality = labels.get(key, (None, None))
                    lines.append(
                        {
                            'gene': dict_line['gene'],
                            'dataset': prod_datasets[tissue],
                            'tissue': tissue,
                            'model': dict_line['model'],
                            'cell_type': dict_line['cell_type'],
                            'factor': dict_line['factor'],
                            'factor_label': label,
                            'factor_quality': quality,
                            'log10_cpk': math.log10(log10_cpk),
                            'log2fc_weighted_vs_all_parent': log2fc,
                            'p_value': np.nextafter(0, 1) if p_value == 0 else p_value
                        }
                    )
        with open(part_path('expression/program', num=idx, hash_=hash_), 'w') as f_out:
            for line in sorted(lines, key=lambda x: (x['tissue'], x['cell_type'], x['gene'], x['p_value'])):
                f_out.write('{}\n'.format(json.dumps(line)))


def build_cell_type_expression(tissues):
    hash_ = random.getrandbits(128)
    for idx, tissue in enumerate(tissues):
        lines = []
        with gzip.open(f'inputs/portal/{tissue}/cell_type_expression.tsv.gz', 'rt') as f:
            header = f.readline().strip().split('\t')
            for line in f:
                dict_line = dict(zip(header, line.strip().split('\t')))
                log10_cpk = float(dict_line['log10_cpk'])
                if log10_cpk > 0.0:
                    log2fc = float(dict_line['log2fc_weighted_vs_all_parent']) if dict_line.get('log2fc_weighted_vs_all_parent', '') != '' else None
                    p_value = float(dict_line['p_value']) if dict_line.get('p_value', '') != '' else None
                    lines.append(
                        {
                            'gene': dict_line['gene'],
                            'dataset': prod_datasets[tissue],
                            'tissue': tissue,
                            'cell_type': dict_line['cell_type'],
                            'log10_cpk': math.log10(log10_cpk),
                            'log2fc_weighted_vs_all_parent': log2fc,
                            'p_value': np.nextafter(0, 1) if p_value == 0 else p_value
                        }
                    )
        with open(part_path('expression/cell_type', num=idx, hash_=hash_), 'w') as f_out:
            for line in sorted(lines, key=lambda x: (x['tissue'], x['gene'], -x['log10_cpk'])):
                f_out.write('{}\n'.format(json.dumps(line)))


def write_gene_blocked(lines, subdir):
    sorted_lines = sorted(lines, key=lambda x: (x['gene'], x['p_value']))
    genes = sorted({line['gene'] for line in sorted_lines})
    blocks = 10 if len(genes) % 10 == 0 else 11
    block_size = len(genes) // 10
    gene_blocks = [genes[idx * block_size:(idx + 1) * block_size] for idx in range(blocks)]
    gene_to_gene_block = {gene: idx for idx in range(blocks) for gene in gene_blocks[idx]}
    hash_ = random.getrandbits(128)
    files = [open(part_path(subdir, num=idx, hash_=hash_), 'w') for idx in range(blocks)]
    for line in sorted_lines:
        files[gene_to_gene_block[line['gene']]].write('{}\n'.format(json.dumps(line)))
    for file in files:
        file.close()


def build_program_expression_by_gene(tissues, labels):
    lines = []
    for tissue in tissues:
        with gzip.open(f'inputs/portal/{tissue}/program_expression.tsv.gz', 'rt') as f:
            header = f.readline().strip().split('\t')
            for line in f:
                dict_line = dict(zip(header, line.strip().split('\t')))
                log10_cpk = float(dict_line['log10_cpk'])
                if log10_cpk > 0.0:
                    log2fc = float(dict_line['log2fc_weighted_vs_all_parent']) if dict_line['log2fc_weighted_vs_all_parent'] != '' else None
                    p_value = float(dict_line['p_value']) if dict_line['p_value'] != '' else None
                    key = (tissue, dict_line['cell_type'], dict_line['factor'])
                    label, quality = labels.get(key, (None, None))
                    lines.append(
                        {
                            'gene': dict_line['gene'],
                            'dataset': prod_datasets[tissue],
                            'tissue': tissue,
                            'model': dict_line['model'],
                            'cell_type': dict_line['cell_type'],
                            'factor': dict_line['factor'],
                            'factor_label': label,
                            'factor_quality': quality,
                            'log10_cpk': math.log10(log10_cpk),
                            'log2fc_weighted_vs_all_parent': log2fc,
                            'p_value': np.nextafter(0, 1) if p_value == 0 else p_value
                        }
                    )
    write_gene_blocked(lines, 'expression-all/program')


def build_cell_state_expression_by_gene(tissues):
    lines = []
    for tissue in tissues:
        with gzip.open(f'inputs/portal/{tissue}/cell_state_expression.tsv.gz', 'rt') as f:
            header = f.readline().strip().split('\t')
            for line in f:
                dict_line = dict(zip(header, line.strip().split('\t')))
                log10_cpk = float(dict_line['log10_cpk'])
                if log10_cpk > 0.0:
                    log2fc = float(dict_line['log2fc_weighted_vs_all_parent']) if dict_line['log2fc_weighted_vs_all_parent'] != '' else None
                    p_value = float(dict_line['p_value']) if dict_line['p_value'] != '' else None
                    lines.append(
                        {
                            'gene': dict_line['gene'],
                            'dataset': prod_datasets[tissue],
                            'tissue': tissue,
                            'cell_type': dict_line['cell_type'],
                            'state_name': dict_line['state_name'],
                            'log10_cpk': math.log10(log10_cpk),
                            'log2fc_weighted_vs_all_parent': log2fc,
                            'p_value': np.nextafter(0, 1) if p_value == 0 else p_value
                        }
                    )
    write_gene_blocked(lines, 'expression-all/cell_state')


def build_program_gene_loadings(tissues, labels):
    lines = []
    for tissue in tissues:
        with gzip.open(f'inputs/portal/{tissue}/program_gene_loadings.tsv.gz', 'rt') as f:
            header = f.readline().strip().split('\t')
            for line in f:
                dict_line = dict(zip(header, line.strip().split('\t')))
                key = (tissue, dict_line['cell_type'], dict_line['factor'])
                label, quality = labels.get(key, (None, None))
                lines.append({
                    'dataset': prod_datasets[tissue],
                    'tissue': tissue,
                    'model': dict_line['model'],
                    'cell_type': dict_line['cell_type'],
                    'factor': dict_line['factor'],
                    'factor_label': label,
                    'factor_quality': quality,
                    'gene': dict_line['gene'],
                    'value': float(dict_line['value']),
                })

    with open(part_path('factors/program/gene'), 'w') as f_out:
        for line in sorted(lines, key=lambda x: (x['tissue'], x['cell_type'], x['factor'], -x['value'])):
            f_out.write('{}\n'.format(json.dumps(line)))


def build_program_qc_gene_set_factor(tissues, labels):
    lines = []
    for tissue in tissues:
        for file in glob.glob(f'inputs/pigean/{prod_datasets[tissue]}/*/pigean.gene_sets.tsv'):
            cell_type = re.findall(r'inputs/pigean/[^/]*/([^/]*)/pigean.gene_sets.tsv', file)[0]
            with open(file, 'rt') as f:
                header = f.readline().strip().split('\t')
                for line in f:
                    dict_line = dict(zip(header, line.strip().split('\t')))
                    if dict_line['beta'] != 'NA' and dict_line['beta'] != '':
                        key = (tissue, cell_type, dict_line['factor'])
                        label, quality = labels.get(key, (None, None))
                        lines.append(
                            {
                                'dataset': prod_datasets[tissue],
                                'tissue': tissue,
                                'cell_type': cell_type,
                                'factor': dict_line['factor'],
                                'factor_label': label,
                                'factor_quality': quality,
                                'gene_set': dict_line['gene_set'],
                                'beta': float(dict_line['beta']),
                                'beta_uncorrected': float(dict_line['beta_uncorrected'])
                            }
                        )

    with open(part_path('factors/program/gene_set'), 'w') as f_out:
        for line in sorted(lines, key=lambda x: (x['tissue'], x['cell_type'], x['factor'], -x['beta'])):
            f_out.write('{}\n'.format(json.dumps(line)))


def build_program_qc_enrichment(tissues, labels):
    lines = []
    for tissue in tissues:
        with gzip.open(f'inputs/portal/{tissue}/program_qc_enrichment.tsv.gz', 'rt') as f:
            header = f.readline().strip().split('\t')
            for line in f:
                dict_line = dict(zip(header, line.strip().split('\t')))
                if dict_line.get('gsea_p', '') == '':
                    continue
                modified_factor = 'Factor{}'.format(re.findall(r'factor_([0-9]*)', dict_line['program_id'])[0])
                key = (tissue, dict_line['cell_type'], modified_factor)
                label, quality = labels.get(key, (None, None))
                lines.append(
                    {
                        'dataset': prod_datasets[tissue],
                        'tissue': tissue,
                        'cell_type': dict_line['cell_type'],
                        'factor': modified_factor,
                        'factor_label': label,
                        'factor_quality': quality,
                        'state_name': dict_line['state_id'],
                        'gsea_p': float(dict_line['gsea_p']),
                        'gsea_q': float(dict_line['gsea_q'])
                    }
                )

    with open(part_path('factors/program/qc'), 'w') as f_out:
        for line in sorted(lines, key=lambda x: (x['tissue'], x['cell_type'], x['factor'], x['gsea_p'])):
            f_out.write('{}\n'.format(json.dumps(line)))


def upload_bioindex_data():
    subprocess.check_call(['aws', 's3', 'cp', '--recursive', 'outputs/gene_program/', f'{s3_bioindex}/gene_program/'])


def main():
    tissues = list_tissues()
    download_portal_data(tissues)
    download_metadata()
    download_pigean_gene_set()
    labels = build_program_labels(tissues)

    build_cell_state_metadata(tissues)
    build_cell_state_metadata_extended(tissues)
    build_qc_metadata()
    build_qc_metadata_extended()
    build_cell_state_expression_by_tissue(tissues)
    build_cell_state_pigean(tissues)
    build_cell_state_heatmap(tissues, labels)
    build_program_factor_metadata(tissues)
    build_program_pigean(tissues, labels)
    build_program_expression_by_dataset(tissues, labels)
    build_cell_type_expression(tissues)
    build_program_expression_by_gene(tissues, labels)
    build_cell_state_expression_by_gene(tissues)
    build_program_gene_loadings(tissues, labels)
    build_program_qc_gene_set_factor(tissues, labels)
    build_program_qc_enrichment(tissues, labels)

    upload_bioindex_data()
    shutil.rmtree('inputs')
    shutil.rmtree('outputs')


if __name__ == '__main__':
    main()
