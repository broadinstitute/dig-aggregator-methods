#!/usr/bin/python3
import argparse
import glob
import json
import os
import re
import shutil
import subprocess

s3_in = 'dig-analysis-data'
s3_out = 'dig-analysis-data'
ldsc_data = '/mnt/var/c2ct/annotation-tissue-biosample'


def get_annotation_tissue_biosamples():
    annotation_tissue_biosamples = []
    for folder in glob.glob(f'{ldsc_data}/*'):
        match = re.findall('.*/(.*)___(.*)___(.*)', folder)
        annotation_tissue_biosamples.append((match[0][0], match[0][1], match[0][2]))
    return annotation_tissue_biosamples


def get_annotation_tissue_biosample_regions(annotation, tissue, biosample):
    key = f'{annotation}___{tissue}___{biosample}'
    out = {}
    with open(f'{ldsc_data}/{key}/{key}.csv', 'r') as f:
        for line in f:
            chromosome, start, end, _ = line.strip().split('\t', 3)
            if chromosome not in out:
                out[chromosome] = []
            out[chromosome].append((int(start), int(end)))
    for chromosome, regions in out.items():
        out[chromosome] = sorted(out[chromosome])
    return out


def get_credible_sets(phenotype, ancestry):
    credible_set_base = f's3://{s3_in}/out/credible_sets/{phenotype}/{ancestry}/merged/part-00000.json'
    tmp_path = f'data/credible_sets/{ancestry}/{phenotype}/'
    subprocess.check_call(['aws', 's3', 'cp', credible_set_base, tmp_path])
    out = {}
    with open(f'{tmp_path}/part-00000.json', 'r') as f:
        for line in f:
            json_line = json.loads(line.strip())
            chromosome = json_line['chromosome']
            if chromosome not in out:
                out[chromosome] = []
            out[chromosome].append((
                json_line['position'],
                json_line['posteriorProbability'],
                json_line['credibleSetId'],
                json_line['source'],
                json_line['dataset']
            ))
    for chromosome, data in out.items():
        out[chromosome] = sorted(data, key=lambda d: (d[0], d[1]))
    return out


def get_chromosome_overlap(credible_set_data, region_data):
    overlap = {}
    curr_cs = 0
    curr_region = 0
    while curr_cs < len(credible_set_data) and curr_region < len(region_data):
        cs = credible_set_data[curr_cs]
        region = region_data[curr_region]
        if cs[0] >= region[1]:
            curr_region += 1
        elif cs[0] < region[0]:
            curr_cs += 1
        else:
            pos, pp, cs_id, source, dataset = cs
            if (source, dataset, cs_id) not in overlap:
                overlap[(source, dataset, cs_id)] = 0.0
            overlap[(source, dataset, cs_id)] += pp
            curr_cs += 1
    return overlap


def get_overlap(credible_set_map, region_map):
    output = dict()
    for chromosome, credible_set_data in credible_set_map.items():
        region_data = region_map.get(chromosome, dict())
        output.update(get_chromosome_overlap(credible_set_data, region_data))
    return output


def get_output(annotation_tissue_biosamples, credible_set_map):
    overlap = {}
    for i, (annotation, tissue, biosample) in enumerate(annotation_tissue_biosamples):
        print(i, annotation, tissue, biosample)
        region_map = get_annotation_tissue_biosample_regions(annotation, tissue, biosample)
        overlap[(annotation, tissue, biosample)] = get_overlap(credible_set_map, region_map)
    return overlap


def write_output(phenotype, ancestry, overlap):
    path_out = f'{s3_out}/out/credible_sets/c2ct/{phenotype}/{ancestry}'
    tmp_file = f'part-00000.json'
    with open(tmp_file, 'w') as f:
        for (annotation, tissue, biosample), data in overlap.items():
            for (source, dataset, credibleSetId), pp in data.items():
                f.write(f'{{"annotation": "{annotation}", "tissue": "{tissue}", "biosample": "{biosample}", '
                        f'"phenotype": "{phenotype}", "ancestry": "{ancestry}", '
                        f'"source": "{source}", "dataset": "{dataset}", "credibleSetId": "{credibleSetId}", '
                        f'"posteriorProbability": {pp}}}\n')
    subprocess.check_call(['touch', '_SUCCESS'])

    # Copy and then remove all data generated in this step
    subprocess.check_call(['aws', 's3', 'cp', tmp_file, f'{path_out}/part-00000.json'])
    subprocess.check_call(['aws', 's3', 'cp', f'_SUCCESS', f'{path_out}/_SUCCESS'])
    os.remove('_SUCCESS')
    os.remove('part-00000.json')
    shutil.rmtree('data')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--phenotype', type=str, required=True)
    parser.add_argument('--ancestry', type=str, required=True)
    args = parser.parse_args()

    credible_set_map = get_credible_sets(args.phenotype, args.ancestry)

    annotation_tissue_biosamples = get_annotation_tissue_biosamples()
    overlap = get_output(annotation_tissue_biosamples, credible_set_map)

    write_output(args.phenotype, args.ancestry, overlap)


# entry point
if __name__ == '__main__':
    main()
