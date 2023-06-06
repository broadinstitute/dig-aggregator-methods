#!/usr/bin/python3
import argparse
import json
import os
import subprocess

data_location = '/mnt/var/basset'
s3_out = 's3://dig-analysis-data/out/basset/translated'


def get_tissue_conversion_map():
    out = {}
    with open(f'{data_location}/basset_tissue_conversion.txt', 'r') as f:
        line = f.readline()
        while len(line) > 0:
            basset_tissue, portal_tissue = line.strip().split(',')
            if portal_tissue not in out:
                out[portal_tissue] = []
            out[portal_tissue].append(basset_tissue)
            line = f.readline()
    return out


def get_part(part):
    subprocess.check_call(['aws', 's3', 'cp', f's3://dig-analysis-data/out/basset/variants/{part}', './data/'])


# Arithmetic mean in lieu of knowing proportion of basset tissues in each portal tissue
def translate_variant(variant_associations, tissue_map):
    out = {'varId': variant_associations['varId']}
    for portal_tissue, basset_tissues in tissue_map.items():
        associations = [variant_associations[basset_tissue] for basset_tissue in basset_tissues]
        out[portal_tissue] = sum(associations) / len(associations)
    return out


def translate_variants(part, tissue_map):
    get_part(part)
    with open(f'{part}', 'w') as f_out:
        with open(f'data/{part}', 'r') as f_in:
            line = f_in.readline()
            while len(line) > 0:
                json_line = json.loads(line)
                translated_line = translate_variant(json_line, tissue_map)
                f_out.write(f'{json.dumps(translated_line)}\n')
                line = f_in.readline()


def upload(part):
    subprocess.check_call(['aws', 's3', 'cp', part, f'{s3_out}/{part}'])
    os.remove(part)


def main():
    opts = argparse.ArgumentParser()
    opts.add_argument('--part')
    args = opts.parse_args()

    tissue_map = get_tissue_conversion_map()
    translate_variants(args.part, tissue_map)
    upload(args.part)


if __name__ == '__main__':
    main()
