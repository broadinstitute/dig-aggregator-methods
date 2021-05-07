#!/usr/bin/python3

import argparse
import concurrent.futures
import json
import os.path
import re
import subprocess
import tempfile

S3DIR = 's3://dig-analysis-data/out/varianteffect'


def colocated_variant(row):
    """
    Find the first colocated variant with a matching allele string or None.
    """
    variants = row.get('colocated_variants', [])

    # find the first with a matching allele string
    return next((v for v in variants if v['allele_string'] == row['allele_string']), None)


def dbSNP(v):
    """
    Extract the rsID for a given, colocated variant.
    """
    rsid = v.get('id')

    # only return if an actual rsID
    return rsid if rsid and rsid.startswith('rs') else None


def allele_frequencies(v, minor_allele):
    """
    Extract allele frequencies for a colocated variant.
    """
    af = v.get('frequencies')

    # no allele frequency data?
    if af is None or minor_allele not in af:
        return {
            'EU': None,
            'HS': None,
            'AA': None,
            'EA': None,
            'SA': None,
        }

    # if found, extract each ancestry
    return af and {
            'EU': af[minor_allele].get('eur'),
            'HS': af[minor_allele].get('amr'),
            'AA': af[minor_allele].get('afr'),
            'EA': af[minor_allele].get('eas'),
            'SA': af[minor_allele].get('sas'),
    }


def common_fields(row):
    """
    Extracts data from the colocated variant fields.
    """
    variant = colocated_variant(row)

    # no colocated variant found, just return the common data
    if not variant:
        return {
            'varId': row['id'],
            'consequence': row['most_severe_consequence'],
            'nearest': row['nearest'],
            'dbSNP': None,
            'maf': None,
            'af': {
                'EU': None,
                'HS': None,
                'AA': None,
                'EA': None,
                'SA': None,
            },
        }

    # get the allele for frequency data
    allele = row['id'].split(':')[-1]

    return {
        'varId': row['id'],
        'consequence': row['most_severe_consequence'],
        'nearest': row['nearest'],
        'dbSNP': dbSNP(variant),
        'maf': variant.get('minor_allele_freq'),
        'af': allele_frequencies(variant, allele),
    }


def process_part(srcdir, outdir, part):
    """
    Download and process a part file.
    """
    _, tmp = tempfile.mkstemp()

    # copy the file into a temp file
    subprocess.check_call(['aws', 's3', 'cp', f'{srcdir}/{part}', tmp])

    # loop over every line, parse, and create common row
    with open(part, mode='w', encoding='utf-8') as out:
        with open(tmp) as fp:
            for line in fp:
                row = json.loads(line)
                common = common_fields(row)

                # write the common record to the output
                if common:
                    json.dump(
                        common,
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


def list_parts(srcdir):
    """
    Returns an array of all the part files that need to be processed.
    """
    lines = subprocess.check_output(['aws', 's3', 'ls', f'{srcdir}/', '--recursive']) \
        .decode('utf-8') \
        .split('\n')

    # only keep actual part files
    parts = [part for part in lines if "/part-" in part]

    # get just the part filename from the output
    return [re.search(r'/(part-[^/]+\.json)$', part).group(1) for part in parts]


def main():
    """
    Arguments: none
    """
    srcdir = f'{S3DIR}/effects'
    outdir = f'{S3DIR}/common'

    # get all the part files that need processed
    parts = list_parts(srcdir)

    with concurrent.futures.ProcessPoolExecutor(max_workers=3) as pool:
        jobs = [pool.submit(process_part, srcdir, outdir, part) for part in parts]

        # wait for all jobs to finish
        concurrent.futures.wait(jobs)


# entry point
if __name__ == '__main__':
    main()
