#!/usr/bin/python3
import json
import os
import subprocess

s3_in = os.environ['INPUT_PATH']
s3_bioindex = os.environ['BIOINDEX_PATH']


def main():
    subprocess.check_call(['aws', 's3', 'cp', f'{s3_in}/genes/gpt_summaries/gene_summaries_gpt.json', '.'])
    with open('gene_summaries_gpt.json', 'r') as f:
        data = {line['gene']: line['abstract'] for line in json.loads(f.read())}
    with open('part-00000.json', 'w') as f:
        for gene in sorted(data, key=lambda k: k.lower()):
            abstract = data[gene]\
                .encode('utf-8').decode()\
                .replace('\t', ' ').replace('\n', '\\n').replace('\"', '\\"').replace('\x1c', '')
            f.write(f'{{"gene": "{gene}", "abstract": "{abstract}"}}\n')
    subprocess.check_call(['aws', 's3', 'cp', 'part-00000.json', f'{s3_bioindex}/gene_summaries/part-00000.json'])


if __name__ == '__main__':
    main()
