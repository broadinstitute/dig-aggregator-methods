#!/usr/bin/python3
import gzip
import json
import os
import subprocess

s3_in = os.environ['INPUT_PATH']
s3_bioindex = os.environ['BIOINDEX_PATH']


def get_info(line):
    return [a.strip() for a in line.strip().split('\t')][8]


def get_info_dict(info):
    d = {}
    for dict_entry in info.split(';'):
        if len(dict_entry) > 0:
            key, value = dict_entry.strip().replace('"', '').split(' ')
            if key == 'tag':
                d[value] = True
            else:
                d[key] = value
    return d


def write_to_bioindex(out):
    with open('part-00000.json', 'w') as f:
        for gene_name, transcript_ids in out.items():
            for (transcript_id, ccds) in transcript_ids:
                line_str = json.dumps({'gene_name': gene_name, 'transcript_id': transcript_id, 'CCDS': ccds})
                f.write(f'{line_str}\n')
    subprocess.check_call(['aws', 's3', 'cp', 'part-00000.json', f'{s3_bioindex}/gene_to_transcript/'])


def main():
    subprocess.check_call(['aws', 's3', 'cp', 's3://dig-analysis-bin/bioindex/gencode.v36lift37.annotation.gtf.gz', './'])
    out = {}
    with gzip.open('gencode.v36lift37.annotation.gtf.gz', 'r') as f:
        line = f.readline().decode()
        while line[0] == '#':
            line = f.readline().decode()
        while len(line) > 0:
            info = get_info(line)
            info_dict = get_info_dict(info)
            if 'gene_name' in info_dict and 'transcript_id' in info_dict:
                gene_name = info_dict['gene_name']
                transcript_id = info_dict['transcript_id'].split('.')[0]
                ccds = info_dict.get('CCDS', False)

                if gene_name not in out:
                    out[gene_name] = []
                if (transcript_id, ccds) not in out[gene_name]:
                    out[gene_name].append((transcript_id, ccds))
            line = f.readline().decode()
    write_to_bioindex(out)


if __name__ == '__main__':
    main()
