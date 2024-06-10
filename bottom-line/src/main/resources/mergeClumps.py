#!/usr/bin/python3

import argparse
import json
import os
import subprocess
import shutil

s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']
meta_types = ['bottom-line', 'min_p', 'largest']


def check_file(path):
    return subprocess.call(f'aws s3 ls {path}', shell=True)


def get_all_variants(phenotype, ancestry):
    for meta_type in meta_types:
        path_in = f'{s3_in}/out/metaanalysis'
        if ancestry == 'TE':
            path_to_variants = f'staging/clumped/analysis/{phenotype}/variants.json'
        else:
            path_to_variants = f'staging/ancestry-clumped/analysis/{phenotype}/ancestry={ancestry}/variants.json'
        path = f'{path_in}/{meta_type}/{path_to_variants}'
        if check_file(path) == 0:
            subprocess.check_call(['aws', 's3', 'cp', path, f'data/{meta_type}/'])


def get_variants_dict():
    variants_dict = {}
    for meta_type in meta_types:
        variants_dict[meta_type] = []
        file = f'data/{meta_type}/variants.json'
        variants = []
        if os.path.exists(file):
            with open(file, 'r') as f:
                first_line = json.loads(f.readline().strip())
                curr_clump = first_line['clump']
                variants.append(first_line)
                for line in f:
                    json_line = json.loads(line.strip())
                    if json_line['clump'] != curr_clump:
                        variants_dict[meta_type].append(variants)
                        variants = []
                        curr_clump = json_line['clump']
                    variants.append(json_line)
        variants_dict[meta_type].append(variants)
    return variants_dict


def get_components(variants_list):
    curr_clump = 0
    clump_to_var_ids = {}
    var_id_to_clump = {}
    for variants in variants_list:
        clump_ids = set()
        for variant in variants:
            if variant not in var_id_to_clump:
                clump_ids |= {curr_clump}
                if curr_clump not in clump_to_var_ids:
                    clump_to_var_ids[curr_clump] = []
                clump_to_var_ids[curr_clump].append(variant)
            else:
                clump_ids |= {var_id_to_clump[variant]}
        if len(clump_ids) > 0:
            clump_to_var_ids, var_id_to_clump = combine_clumps(clump_to_var_ids, var_id_to_clump, clump_ids)
            curr_clump += 1
    return var_id_to_clump


def combine_clumps(clump_to_var_ids, var_id_to_clump, clump_ids):
    min_clump_id = min(clump_ids)
    new_clump = []
    for clump_id in clump_ids:
        new_clump += clump_to_var_ids[clump_id]
        clump_to_var_ids.pop(clump_id)
    clump_to_var_ids[min_clump_id] = new_clump

    for variant in clump_to_var_ids[min_clump_id]:
        var_id_to_clump[variant] = min_clump_id

    return clump_to_var_ids, var_id_to_clump


def get_clump_to_metas(variants_dict, var_id_to_clump):
    clump_to_metas = {}
    for meta_type, variant_list in variants_dict.items():
        for variants in variant_list:
            for variant in variants:
                clump = var_id_to_clump[variant['varId']]
                if clump not in clump_to_metas:
                    clump_to_metas[clump] = {meta_type: False for meta_type in meta_types}
                clump_to_metas[clump][meta_type] = True
    for clump, metas in clump_to_metas.items():
        clump_to_metas[clump] = ';'.join([meta_type for meta_type in meta_types if metas[meta_type]])
    return clump_to_metas


def get_overview(clump_to_meta):
    output = {}
    for clump, metas in clump_to_meta.items():
        if metas not in output:
            output[metas] = 0
        output[metas] += 1
    return output


def output_and_upload(phenotype, ancestry, variants_dict, var_id_to_clump, clump_to_metas, overview):
    if ancestry == 'TE':
        path_out = f'{s3_out}/out/metaanalysis/bottom-line/staging/merged/analysis/{phenotype}/'
    else:
        path_out = f'{s3_out}/out/metaanalysis/bottom-line/staging/ancestry-merged/analysis/{phenotype}/ancestry={ancestry}/'
    os.mkdir('output')
    output_and_upload_variants(path_out, variants_dict, var_id_to_clump, clump_to_metas)
    output_and_upload_overview(path_out, overview)
    cleanup()


def output_and_upload_variants(path_out, variants_dict, var_id_to_clump, clump_to_metas):
    variants_list = variants_dict['bottom-line']
    if sum([len(variants) for variants in variants_list]) > 0:
        file = f'output/variants.json'
        with open(file, 'w') as f:
            for variants in variants_list:
                for variant in variants:
                    variant['inMetaTypes'] = clump_to_metas[var_id_to_clump[variant['varId']]]
                    f.write('{}\n'.format(json.dumps(variant)))
        subprocess.check_call(['aws', 's3', 'cp', file, path_out])


def output_and_upload_overview(path_out, overview):
    if len(overview) > 0:
        file = f'output/overview.tsv'
        with open(file, 'w') as f:
            for category in sorted(overview, key=lambda category: -overview[category]):
                f.write('{}\t{}\n'.format(category, overview[category]))
        subprocess.check_call(['aws', 's3', 'cp', file, path_out])


def cleanup():
    if os.path.exists('data'):
        shutil.rmtree('data')
    if os.path.exists('output'):
        shutil.rmtree('output')


def main():
    arg_parser = argparse.ArgumentParser(prog='huge-common.py')
    arg_parser.add_argument("--phenotype", help="Phenotype (e.g. T2D) to run", required=True)
    arg_parser.add_argument("--ancestry", help="Phenotype (e.g. T2D) to run", required=True)
    args = arg_parser.parse_args()

    get_all_variants(args.phenotype, args.ancestry)
    variants_dict = get_variants_dict()
    all_variants = [[line['varId'] for line in lines] for meta_type in meta_types for lines in variants_dict[meta_type]]
    if sum([len(variants) for variants in all_variants]) > 0:
        var_id_to_clump = get_components(all_variants)
        clump_to_metas = get_clump_to_metas(variants_dict, var_id_to_clump)
        overview = get_overview(clump_to_metas)
        output_and_upload(args.phenotype, args.ancestry, variants_dict, var_id_to_clump, clump_to_metas, overview)


if __name__ == '__main__':
    main()
