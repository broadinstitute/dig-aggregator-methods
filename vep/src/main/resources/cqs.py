#!/usr/bin/python3

import argparse
import json
import os
import re
import subprocess
import tempfile

s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']


def rename_cqs_field(s):
    """
    Lifted from BioIndex code to camelCase the names of consequences.
    """
    s=re.sub(r'(?:[^a-z0-9]+)(.)', lambda m: m.group(1).upper(), s, flags=re.IGNORECASE)
    s=re.sub(r'^[A-Z][a-z]+', lambda m: m.group(0).lower(), s)

    return s


def exploded_consequences(row):
    """
    Extracts transcript consequences from the row.
    """
    for cqs in row.get('transcript_consequences', []):
        record = dict()

        # include the variant ID and locus with each consequence
        record['varId'] = row['id']
        record['chromosome'] = row['seq_region_name']
        record['position'] = int(row['start'])

        # apend all the consequence fields; ignore some
        for k, v in cqs.items():
            if k not in ['domains']:
                record[rename_cqs_field(k)] = v

        # add the record to the returned values
        yield record


def process_part(srcdir, outdir, part):
    tmp = tempfile.TemporaryDirectory()

    base_part, part_ext = os.path.splitext(part)

    # copy the file into a temp file
    subprocess.check_call(['aws', 's3', 'cp', f'{srcdir}/{part}', f'{tmp.name}/input/'])
    subprocess.check_call(f'zstd --rm -d {tmp.name}/input/{part}', shell=True)

    # loop over every line, parse, and create common row
    with open(f'{tmp.name}/input/{base_part}') as fp:
        os.makedirs(f'{tmp.name}/output', exist_ok=True)
        with open(f'{tmp.name}/output/{base_part}', mode='w', encoding='utf-8') as out:
            for line in fp:
                row = json.loads(line)

                # write the common record to the output
                for cqs in exploded_consequences(row):
                    json.dump(
                        cqs,
                        out,
                        separators=(',', ':'),
                        check_circular=False,
                    )

                    # output a newline after the json
                    print(file=out)

    # copy the output file to S3
    subprocess.check_call(f'zstd --rm {tmp.name}/output/{base_part}', shell=True)
    subprocess.check_call(['aws', 's3', 'cp', f'{tmp.name}/output/{part}', f'{outdir}/{part}'])

    # cleanup
    tmp.cleanup()

    # debug output
    print(f'Processed {part} successfully')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--part', type=str, required=True, help="part name")
    parser.add_argument('--data-type', type=str, required=True, help="data type (e.g. variants)")
    args = parser.parse_args()

    # s3 locations
    srcdir = f'{s3_in}/out/varianteffect/{args.data_type}/cqs-effects'
    outdir = f'{s3_out}/out/varianteffect/{args.data_type}/cqs'

    # run
    process_part(srcdir, outdir, args.part)


# entry point
if __name__ == '__main__':
    main()
