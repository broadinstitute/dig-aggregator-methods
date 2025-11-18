#!/usr/bin/python3

import argparse
import json
import os
import subprocess
import tempfile

s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']


def pick_transcript_consequence(row):
    tc = row.get('transcript_consequences', [])
    if len(tc) == 1:
        return {
            'consequenceGeneId': tc[0].get('gene_id'),
            'consequenceGeneSymbol': tc[0].get('gene_symbol'),
            'consequenceImpact': tc[0].get('impact')
        }
    else:
        return {}


def colocated_variant(row, ref, alt):
    """
    Find the first colocated variant with a matching allele string or None.
    """
    co = row.get('colocated_variants', [])

    # only keep colocated variants where the minor allele can be ref or alt
    variants = [v for v in co if len(v.get('frequencies', {}).keys() & {ref, alt}) > 0]
    # Prefer variants with rs ID defined
    rs_variants = [v for v in variants if v.get('id', '').startswith('rs')]
    return next(iter(rs_variants + variants + co), None)


def dbSNP(v):
    """
    Extract the rsID for a given, colocated variant.
    """
    rsid = v.get('id')

    # only return if an actual rsID
    return rsid if rsid and rsid.startswith('rs') else None


def allele_frequencies(v, ref, alt):
    """
    Extract allele frequencies for a colocated variant.
    """
    af = v.get('frequencies', {})

    # find the matching allele in the frequency map (prefer alt)
    allele = next((a for a in [alt, ref] if a in af), None)

    # no allele frequency data?
    if not allele:
        return {
            'EU': None,
            'HS': None,
            'AA': None,
            'EA': None,
            'SA': None,
        }

    # lookup the frequency data
    def get_freq(*cols):
        freqs = [af[allele].get(c) for c in cols]

        # find the first non-null/zero frequency from the columns
        freq = next((f for f in freqs if f), None)

        # flip the frequency if this is the reference allele
        return 1.0 - freq if allele == ref else freq

    # try gnomad, if not there use 1kg
    return {
        'EU': get_freq('gnomade_nfe', 'gnomade_nfe', 'eur'),
        'HS': get_freq('gnomade_amr', 'gnomadg_amr', 'amr'),
        'AA': get_freq('gnomade_afr', 'gnomadg_afr', 'afr'),
        'EA': get_freq('gnomade_eas', 'gnomadg_eas', 'eas'),
        'SA': get_freq('gnomade_sas', 'gnomadg_sas', 'sas'),
    }


def common_fields(row):
    """
    Extracts data from the colocated variant fields.
    """
    ref, alt = row['id'].split(':')[-2:]

    # find the correct colocated variant for this allele
    variant = colocated_variant(row, ref, alt)

    out = {
        'varId': row['id'],
        'consequence': row['most_severe_consequence'],
        'nearest': row['nearest'],
        'chromosome': row['seq_region_name'],
        'position': int(row['start'])
    }
    for k, v in pick_transcript_consequence(row).items():
        out[k] = v

    # no colocated variant found, just return the common data
    if not variant:
        return out

    out['dbSNP'] = dbSNP(variant)
    out['af'] = allele_frequencies(variant, ref, alt)
    freq_ancestry = next((ancestry for ancestry in ['EU', 'HS', 'AA', 'EA', 'SA'] if out['af'][ancestry] is not None), None)
    if freq_ancestry is not None:
        if out['af'][freq_ancestry] <= 0.5:
            out['minorAllele'] = alt
            out['maf'] = out['af'][freq_ancestry]
        else:
            out['minorAllele'] = ref
            out['maf'] = 1 - out['af'][freq_ancestry]

    return out


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
    srcdir = f'{s3_in}/out/varianteffect/{args.data_type}/common-effects'
    outdir = f'{s3_out}/out/varianteffect/{args.data_type}/common'

    # run
    process_part(srcdir, outdir, args.part)


# entry point
if __name__ == '__main__':
    main()
