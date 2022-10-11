#!/usr/bin/python3
import gzip
import json
import subprocess


outdir = 's3://dig-bio-index/gene_to_transcript/'


def get_info(line):
    return [a.strip() for a in line.strip().split('\t')][8]


def get_info_dict(info):
    return dict([a.strip().replace('"', '').split(' ') for a in info.split(';')][:-1])


def write_to_bioindex(out):
    with open('part-00000.json', 'w') as f:
        for gene_name, transcript_ids in out.items():
            for transcript_id in transcript_ids:
                line_str = json.dumps({'gene_name': gene_name, 'transcript_id': transcript_id})
                f.write(f'{line_str}\n')
    subprocess.check_call(['aws', 's3', 'cp', 'part-00000.json', outdir])


def main():
    subprocess.check_call(['aws', 's3', 'cp', 's3://dig-analysis-data/raw/gencode.v36lift37.annotation.gtf.gz', './'])
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

                if gene_name not in out:
                    out[gene_name] = []
                if transcript_id not in out[gene_name]:
                    out[gene_name].append(transcript_id)
            line = f.readline().decode()
    write_to_bioindex(out)


if __name__ == '__main__':
    main()
