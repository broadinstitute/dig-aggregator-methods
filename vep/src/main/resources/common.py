#!/usr/bin/python3

import argparse
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

    # if found, extract each ancestry
    return af and {
            'EU': af[minor_allele].get('eur'),
            'HS': af[minor_allele].get('amr'),
            'AA': af[minor_allele].get('afr'),
            'EA': af[minor_allele].get('eas'),
            'SA': af[minor_allele].get('sas'),
    }

    # only return if an actual rsID
    return rsid if rsid and rsid.startswith('rs') else None


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
            'af': None,
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


def main():
    """
    Arguments: part-file
    """
    opts = argparse.ArgumentParser()
    opts.add_argument('part')

    # parse cli
    args = opts.parse_args()

    # separate the part filename from the source
    _, part = os.path.split(args.part)

    # where to write the output to
    srcdir = f'{S3DIR}/effects'
    outdir = f'{S3DIR}/common'

    # copy the part file locally
    subprocess.check_call(['aws', 's3', 'cp', f'{srcdir}/{part}', 'tmp.json'])

    # loop over every line, parse, and create common row
    with open(part, mode='w', encoding='utf-8') as out:
        with open('tmp.json') as fp:
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
    os.remove('tmp.json')
    os.remove(outfile)


# entry point
if __name__ == '__main__':
    main()
