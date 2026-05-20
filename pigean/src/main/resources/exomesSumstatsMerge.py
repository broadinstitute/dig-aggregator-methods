#!/usr/bin/python3
import argparse
import json
import os
import re
import subprocess

downloaded_files = '/mnt/var/pigean'

s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']

code_map = {
    'ICD9CM': 'ICD9',
    'ICD10CM': 'ICD10CM'
}
def get_phecode_map():
    phecode_map = {}
    with open(f'{downloaded_files}/phecodeX_R_map.csv', 'r') as f:
        _ = f.readline()
        for line in f:
            code, code_type, phecode_id = line.strip().replace('"', '').split(',')
            phecode = f'phecode_{phecode_id}'
            if phecode not in phecode_map:
                phecode_map[phecode] = []
            phecode_map[phecode].append(f'{code_map[code_type]}:{code}')
    return phecode_map


def get_xrefs_to_xrefs_map():
    xrefs_to_xrefs_map = {}
    with open(f'{downloaded_files}/mondo.json', 'r') as f:
        mondo_data = json.load(f)
    for node in mondo_data['graphs'][0]['nodes']:
        maybe_top_id = re.findall(r'http://purl.obolibrary.org/obo/(.*)', node['id'])
        if len(maybe_top_id) > 0 and 'meta' in node:
            top_id = maybe_top_id[0].replace('_', ':')
            xrefs = [xref['val'] for xref in node['meta'].get('xrefs', [])] + [top_id]
            for xref in xrefs:
                if xref not in xrefs_to_xrefs_map:
                    xrefs_to_xrefs_map[xref] = []
                xrefs_to_xrefs_map[xref] += xrefs
    return xrefs_to_xrefs_map


def get_gcat_map():
    with open(f'{downloaded_files}/gcat_map.json', 'r') as f:
        return json.load(f)


def get_mapped_trait(trait_group, trait, gcat_map):
    if trait_group == 'portal':
        return trait
    elif trait_group == 'gcat_trait':
        return gcat_map.get(trait)


match_score = {
    'skos:exactMatch': 0,
    'oboInOwl:hasDbXref': 0,
    'skos:relatedMatch': 1,
    'skos:broadMatch': 1,
    'skos:closeMatch': 2
}
trait_group_score = {
    'portal': 0,
    'gcat_trait': 1
}
def get_portal_map():
    gcat_map = get_gcat_map()
    portal_map = {}
    with open(f'{downloaded_files}/portal_phenotypes_flat.tsv', 'r') as f:
        header = f.readline().strip().split('\t')
        for line in f:
            line_dict = dict(zip(header, line.strip().split('\t')))
            target_id = line_dict['target_id']
            if target_id not in portal_map:
                portal_map[target_id] = []
            trait_group = line_dict['gwas_source_category']
            if trait_group in trait_group_score:
                score = (trait_group_score[trait_group], match_score[line_dict['mapping_predicate']])
                mapped_trait = get_mapped_trait(trait_group, line_dict['phenotype'], gcat_map)
                if mapped_trait is not None:
                    portal_map[target_id].append((score, trait_group, mapped_trait))
    return portal_map


def get_matched_traits(phecode):
    phecode_map = get_phecode_map()
    xrefs_to_xrefs_map = get_xrefs_to_xrefs_map()
    portal_map = get_portal_map()
    ICD_keys = phecode_map.get(phecode, [])
    traits = []
    for ICD_key in ICD_keys:
        for xref_key in xrefs_to_xrefs_map.get(ICD_key, []):
            traits += portal_map.get(xref_key, [])
    return traits


# using the list of traits, filter to the highest priority, and then filter to traits with the highest degeneracy
def select_top_traits(traits):
    min_score = sorted(traits)[0][0]
    filtered_traits = [(trait[1], trait[2]) for trait in traits if trait[0] == min_score]
    grouped_traits = {}
    for trait in filtered_traits:
        if trait not in grouped_traits:
            grouped_traits[trait] = 0
        grouped_traits[trait] += 1
    max_group_size = max(grouped_traits.values())
    return [trait for trait in grouped_traits if grouped_traits[trait] == max_group_size]


def get_trait_to_size_map():
    trait_to_size_map = {}
    for trait in subprocess.check_output(['aws', 's3', 'ls', f'{s3_in}/out/pigean/inputs/sumstats/', '--recursive']).decode().strip().split('\n'):
        _, _, size_str, path = trait.strip().split()
        if 'pigean.sumstats.gz' in path:
            _, _, _, _, trait_group, trait, _ = path.split('/')
            trait_to_size_map[(trait_group, trait)] = int(size_str)
    return trait_to_size_map


def check_existence(path):
    return subprocess.call(['aws', 's3', 'ls', path, '--recursive'])


def upload(exomes_group, exomes_trait, trait_group, trait):
    exomes_path = f'{s3_in}/out/pigean/inputs/exomes/{exomes_group}/{exomes_trait}'
    trait_path = f'{s3_in}/out/pigean/inputs/sumstats/{trait_group}/{trait}'
    output_path = f'{s3_out}/out/pigean/inputs/exomes___sumstats/{exomes_group}___{trait_group}/{exomes_trait}___{trait}'
    if not check_existence(trait_path):
        subprocess.check_call(['aws', 's3', 'cp', f'{exomes_path}/', f'{output_path}/', '--recursive'])
        subprocess.check_call(['aws', 's3', 'cp', f'{trait_path}/', f'{output_path}/', '--recursive'])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--exomes-group', default=None, required=True, type=str)
    parser.add_argument('--exomes-trait', default=None, required=True, type=str)
    args = parser.parse_args()

    trait_to_size_map = get_trait_to_size_map()
    matched_traits = get_matched_traits(args.exomes_trait)
    filtered_traits = [trait for trait in matched_traits if (trait[1], trait[2]) in trait_to_size_map]
    if len(filtered_traits) > 0:
        top_traits = select_top_traits(filtered_traits)
        if len(top_traits) > 1:
            top_trait = sorted(top_traits, key = lambda trait: trait_to_size_map[trait], reverse=True)[0]
        else:
            top_trait = top_traits[0]
        upload(args.exomes_group, args.exomes_trait, top_trait[0], top_trait[1])


if __name__ == '__main__':
    main()
