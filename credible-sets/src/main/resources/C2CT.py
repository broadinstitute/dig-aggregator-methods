#!/usr/bin/python3
import argparse
import glob
import json
import os
import re
import shutil
import subprocess

s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']
ldsc_data = '/mnt/var/c2ct'


def get_annotation_tissue_biosamples():
    annotation_tissue_biosamples = []
    for folder in glob.glob(f'{ldsc_data}/annotation-tissue-biosample/*'):
        match = re.findall('.*/(.*)___(.*)___(.*)', folder)
        annotation_tissue_biosamples.append((match[0][0], match[0][1], match[0][2]))
    for folder in glob.glob(f'{ldsc_data}/annotation-tissue/*'):
        match = re.findall('.*/(.*)___(.*)', folder)
        annotation_tissue_biosamples.append((match[0][0], match[0][1], None))
    return annotation_tissue_biosamples


def get_path(annotation, tissue, biosample):
    if biosample is not None:
        key = f'{annotation}___{tissue}___{biosample}'
        return f'{ldsc_data}/annotation-tissue-biosample/{key}/{key}.csv'
    else:
        key = f'{annotation}___{tissue}'
        return f'{ldsc_data}/annotation-tissue/{key}/{key}.csv'


def get_annotation_tissue_biosample_regions(annotation, tissue, biosample):
    out = {}
    annotation_size = 0
    with open(get_path(annotation, tissue, biosample), 'r') as f:
        for line in f:
            chromosome, start, end, _ = line.strip().split('\t', 3)
            if chromosome not in out:
                out[chromosome] = []
            out[chromosome].append((int(start), int(end)))
            annotation_size += int(end) - int(start)
    for chromosome, regions in out.items():
        out[chromosome] = sorted(out[chromosome])
    return out, annotation_size


def get_credible_sets(phenotype, ancestry):
    credible_set_base = f'{s3_in}/out/credible_sets/merged/{phenotype}/{ancestry}/part-00000.json'
    tmp_path = f'data/credible_sets/{ancestry}/{phenotype}/'
    subprocess.check_call(['aws', 's3', 'cp', credible_set_base, tmp_path])
    out = {}
    cs_data = {}
    with open(f'{tmp_path}/part-00000.json', 'r') as f:
        for line in f:
            json_line = json.loads(line.strip())
            chromosome = json_line['chromosome']
            if chromosome not in out:
                out[chromosome] = []
            out[chromosome].append((
                json_line['position'],
                json_line['posteriorProbability'],
                json_line['credibleSetId']
            ))
            if bool(json_line['leadSNP']):
                cs_data[json_line['credibleSetId']] = (
                    json_line['source'],
                    json_line['dataset'],
                    json_line['chromosome'],
                    json_line['clumpStart'],
                    json_line['clumpEnd'],
                    json_line['varId']
                )
    for chromosome, data in out.items():
        out[chromosome] = sorted(data, key=lambda d: (d[0], d[1]))
    return out, cs_data


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
            pos, pp, cs_id = cs
            if cs_id not in overlap:
                overlap[cs_id] = (0.0, 0)
            curr_pp, curr_count = overlap[cs_id]
            overlap[cs_id] = (curr_pp + pp, curr_count + 1)
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
    annotation_sizes = {}
    for i, (annotation, tissue, biosample) in enumerate(annotation_tissue_biosamples):
        print(i, annotation, tissue, biosample)
        region_map, annotation_size = get_annotation_tissue_biosample_regions(annotation, tissue, biosample)
        annotation_sizes[(annotation, tissue, biosample)] = annotation_size
        overlap[(annotation, tissue, biosample)] = get_overlap(credible_set_map, region_map)
    return overlap, annotation_sizes


def write_output(phenotype, ancestry, overlap, credible_set_data, annotation_sizes):
    path_out = f'{s3_out}/out/credible_sets/c2ct/{phenotype}/{ancestry}'
    tmp_file = f'part-00000.json'
    with open(tmp_file, 'w') as f:
        for (annotation, tissue, biosample), data in overlap.items():
            annot_size = annotation_sizes[(annotation, tissue, biosample)]
            for credible_set_id, (pp, count) in data.items():
                source, dataset, chromosome, clump_start, clump_end, lead_snp = credible_set_data[credible_set_id]
                biosample_str = 'null' if biosample is None else f'"{biosample}"'
                pp = max(min(pp, 1.0), 0.0)
                f.write(f'{{"annotation": "{annotation}", "tissue": "{tissue}", "biosample": {biosample_str}, '
                        f'"phenotype": "{phenotype}", "ancestry": "{ancestry}", '
                        f'"source": "{source}", "dataset": "{dataset}", "credibleSetId": "{credible_set_id}", '
                        f'"chromosome": "{chromosome}", "clumpStart": {clump_start}, "clumpEnd": {clump_end}, '
                        f'"leadSNP": "{lead_snp}", "posteriorProbability": {pp}, '
                        f'"varCount": {count}, "annot_bp": {annot_size}}}\n')
    subprocess.check_call(['touch', '_SUCCESS'])

    # Copy and then remove all data generated in this step
    subprocess.check_call(['aws', 's3', 'cp', tmp_file, f'{path_out}/part-00000.json'])
    subprocess.check_call(['aws', 's3', 'cp', f'_SUCCESS', f'{path_out}/_SUCCESS'])
    os.remove('_SUCCESS')
    os.remove(tmp_file)
    shutil.rmtree('data')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--phenotype', type=str, required=True)
    parser.add_argument('--ancestry', type=str, required=True)
    args = parser.parse_args()

    credible_set_map, credible_set_data = get_credible_sets(args.phenotype, args.ancestry)

    annotation_tissue_biosamples = get_annotation_tissue_biosamples()
    overlap, annotation_sizes = get_output(annotation_tissue_biosamples, credible_set_map)

    write_output(args.phenotype, args.ancestry, overlap, credible_set_data, annotation_sizes)


# entry point
if __name__ == '__main__':
    main()
