#!/usr/bin/python3

import argparse
import json
import os.path
import re
import subprocess
import tempfile

S3DIR = 's3://dig-analysis-pxs/out/varianteffect'


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
    """
    Download and process a part file.
    """
    _, tmp = tempfile.mkstemp()

    # copy the file into a temp file
    subprocess.check_call(['aws', 's3', 'cp', f'{srcdir}/{part}', tmp])

    # loop over every line, parse, and create common row
    with open(tmp) as fp:
        with open(part, mode='w', encoding='utf-8') as out:
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
    subprocess.check_call(['aws', 's3', 'cp', part, f'{outdir}/{part}'])

    # cleanup
    os.remove(tmp)
    os.remove(part)

    # debug output
    print(f'Processed {part} successfully')


def main():
    """
    Arguments: part-file
    """
    opts = argparse.ArgumentParser()
    opts.add_argument('part')

    # parse cli
    args = opts.parse_args()

    # s3 locations
    srcdir = f'{S3DIR}/effects'
    outdir = f'{S3DIR}/cqs'

    # run
    process_part(srcdir, outdir, args.part)


# entry point
if __name__ == '__main__':
    main()
