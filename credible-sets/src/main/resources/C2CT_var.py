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
ldsc_data = '/mnt/var/c2ct/annot'


def get_project_annotation_tissue_biosamples():
    project_annotation_tissue_biosamples = []
    for folder in glob.glob(f'{ldsc_data}/*/*'):
        match = re.findall('.*/(.*)/(.*)___(.*)___(.*)', folder)
        project_annotation_tissue_biosamples.append((match[0][0], match[0][1], match[0][2], match[0][3]))
    return project_annotation_tissue_biosamples


def get_path(project, annotation, tissue, biosample):
    key = f'{annotation}___{tissue}___{biosample}'
    return f'{ldsc_data}/{project}/{key}/{key}.csv'


def get_annotation_tissue_biosample_regions(project, annotation, tissue, biosample):
    out = {}
    annotation_size = 0
    with open(get_path(project, annotation, tissue, biosample), 'r') as f:
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
            if json_line['source'] == 'credible_set':
                chromosome = json_line['chromosome']
                if chromosome not in out:
                    out[chromosome] = []
                out[chromosome].append((
                    json_line['position'],
                    json_line['posteriorProbability'],
                    json_line['credibleSetId'],
                    json_line.get('pValue', 1.0),
                    json_line['varId']
                ))
                if json_line['credibleSetId'] not in cs_data:
                    cs_data[json_line['credibleSetId']] = {
                        'source': json_line['source'],
                        'dataset': json_line['dataset'],
                        'chromosome': json_line['chromosome'],
                        'clumpStart': json_line['clumpStart'],
                        'clumpEnd': json_line['clumpEnd'],
                        'inMetaTypes': json_line.get('inMetaTypes', 'credible-set'),
                        'varTotal': 0
                    }
                cs_data[json_line['credibleSetId']]['varTotal'] += 1
                if bool(json_line['leadSNP']):
                    cs_data[json_line['credibleSetId']]['leadSNP'] = json_line['varId']
                    cs_data[json_line['credibleSetId']]['leadSNPPValue'] = json_line.get('pValue', 1.0)
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
            pos, pp, cs_id, p_value, var_id = cs
            if cs_id not in overlap:
                overlap[cs_id] = []
            overlap[cs_id].append((var_id, p_value, pp))
            curr_cs += 1
    return overlap


def get_overlap(credible_set_map, region_map):
    output = dict()
    for chromosome, credible_set_data in credible_set_map.items():
        region_data = region_map.get(chromosome, dict())
        output.update(get_chromosome_overlap(credible_set_data, region_data))
    return output


def get_output(project_annotation_tissue_biosamples, credible_set_map):
    overlap = {}
    annotation_sizes = {}
    for i, (project, annotation, tissue, biosample) in enumerate(project_annotation_tissue_biosamples):
        print(i, project, annotation, tissue, biosample)
        region_map, annotation_size = get_annotation_tissue_biosample_regions(project, annotation, tissue, biosample)
        annotation_sizes[(project, annotation, tissue, biosample)] = annotation_size
        cs_overlap_data = get_overlap(credible_set_map, region_map)
        for cs_id, cs_id_data in cs_overlap_data.items():
            if cs_id not in overlap:
                overlap[cs_id] = {}
            overlap[cs_id][(project, annotation, tissue, biosample)] = cs_id_data
    return overlap, annotation_sizes


def write_output(phenotype, ancestry, overlap, credible_set_data, annotation_sizes):
    path_out = f'{s3_out}/out/credible_sets/c2ct_var/{phenotype}/{ancestry}'
    tmp_file = f'part-00000.json'
    with open(tmp_file, 'w') as f:
        for credible_set_id, data in overlap.items():
            cs_data = credible_set_data[credible_set_id]
            for (project, annotation, tissue, biosample), var_ids in data.items():
                annot_size = annotation_sizes[(project, annotation, tissue, biosample)]
                biosample_str = 'null' if biosample is None else f'"{biosample}"'
                for (var_id, pvalue, pp) in var_ids:
                    pp = max(min(pp, 1.0), 0.0)
                    f.write(f'{{"project": "{project}", '
                            f'"annotation": "{annotation}", "tissue": "{tissue}", "biosample": {biosample_str}, '
                            f'"phenotype": "{phenotype}", "ancestry": "{ancestry}", '
                            f'"source": "{cs_data["source"]}", "inMetaTypes": "{cs_data["inMetaTypes"]}", '
                            f'"dataset": "{cs_data["dataset"]}", '
                            f'"credibleSetId": "{credible_set_id}", "chromosome": "{cs_data["chromosome"]}", '
                            f'"clumpStart": {cs_data["clumpStart"]}, "clumpEnd": {cs_data["clumpEnd"]}, '
                            f'"varID": "{var_id}", "posteriorProbability": {pp}, "pValue": {pvalue}, '
                            f'"annot_bp": {annot_size}}}\n')
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

    project_annotation_tissue_biosamples = get_project_annotation_tissue_biosamples()
    overlap, annotation_sizes = get_output(project_annotation_tissue_biosamples, credible_set_map)

    write_output(args.phenotype, args.ancestry, overlap, credible_set_data, annotation_sizes)


# entry point
if __name__ == '__main__':
    main()
