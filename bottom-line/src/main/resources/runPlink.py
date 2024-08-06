#!/usr/bin/python3
import argparse
import glob
import json
from multiprocessing import Pool
import numpy as np
import os
import pandas as pd
import subprocess

from scipy.sparse import lil_matrix
from scipy.sparse.csgraph import connected_components


s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']
CLUMPING_ROOT = f'/mnt/var/clumping'

import_threads = 8

params_by_type = {
    'analysis': {'p1': 5E-8, 'p2': 5E-6, 'r2': 0.01, 'kb': 5000}
}

# Each of the ancestries should only be run once. Runs against trans-ethnic results
TRANS_ETHNIC_ANCESTRIES = {
    'AA': 'afr',
    'EU': 'eur',
    'HS': 'amr',
    'EA': 'eas',
    'SA': 'sas'
}

# ancestry mapping portal -> g1000 for all possible ancestries
G1000_ANCESTRIES_BY_PORTAL_ANCESTRIES = {
    'AA': {'AA': 'afr'},
    'AF': {'AF': 'afr'},
    'SSAF': {'SSAF': 'afr'},
    'EU': {'EU': 'eur'},
    'HS': {'HS': 'amr'},
    'EA': {'EA': 'eas'},
    'SA': {'SA': 'sas'},
    'GME': {'GME': 'sas'},
    'Mixed': TRANS_ETHNIC_ANCESTRIES,
    'TE': TRANS_ETHNIC_ANCESTRIES
}


def download(s3_file):
    """
    Copy a file from S3 to here with the same name."
    """
    subprocess.check_call(['aws', 's3', 'cp', '--recursive', s3_file, '.'])
    for fn in glob.glob('part-*'):
        subprocess.check_call(['zstd', '-d', '--rm', fn])


def upload(local_file, s3_dir):
    """
    Copy a local file to S3.
    """
    subprocess.check_call(['aws', 's3', 'cp', local_file, f'{s3_dir}/{local_file}'])


def load_individual_bottom_line(data):
    file, params = data
    lines = []
    cols = ['varId', 'pValue']
    with open(file, 'r') as f:
        for line in f:
            json_line = json.loads(line, parse_float=np.float64)
            lines.append([json_line[col] for col in cols])
    df = pd.DataFrame(data=lines, columns=['varId', 'pValue'])

    # explode varId to get chrom, pos, ref, and alt (alt will be catch all for everything else in the string)
    df[['chromosome', 'position', 'reference', 'alt']] = df['varId'].str.split(':', n=3, expand=True)

    # drop unnecessary columns and cast types
    df = df.drop(['reference', 'alt'], axis=1)
    df = df.astype({
        'position': np.int64,
        'pValue': np.float64,
    })

    # filter variants within absolute limit, and drop p=0 associations
    return df[df['pValue'] <= params['p2']]


def load_bottom_line(s3_dir, params):
    # load the dataframe, ensure p-values are high-precision
    download(s3_dir)
    inputs = [(fn, params) for fn in glob.glob('part-*')]
    with Pool(import_threads) as p:
        dfs = p.map(load_individual_bottom_line, inputs)
    return pd.concat(dfs)


def build_assoc_file(assoc_file, df):
    """
    Rename columns, select, and write file.
    """
    df = df.rename(columns={
        'dbSNP': 'SNP',
        'chromosome': 'CHR',
        'position': 'BP',
        'pValue': 'P',
    })

    # set the column order for the output file and write it
    df[['CHR', 'SNP', 'BP', 'P']].to_csv(assoc_file, sep='\t', index=False)


def run_plink(assoc_file, outdir, ancestries, params):
    """
    Run plink for each ancestry. Uploads results to S3.
    """
    for ancestry, g1000_ancestry in ancestries.items():
        g1000=f'g1000_{g1000_ancestry}'

        # process this ancestry; ignore errors
        subprocess.run([
            f'{CLUMPING_ROOT}/plink',
            '--bfile',
            f'{CLUMPING_ROOT}/{g1000}/{g1000}',
            '--clump-p1',
            str(params['p1']),
            '--clump-p2',
            str(params['p2']),
            '--clump-r2',
            str(params['r2']),
            '--clump-kb',
            str(params['kb']),
            '--clump',
            assoc_file,
        ])

        # upload the log if it exists
        if os.path.isfile('plink.log'):
            upload('plink.log', f'{outdir}/plink.{ancestry}.log')

        # upload and rename the clumped file if it exists
        if os.path.isfile('plink.clumped'):
            os.rename('plink.clumped', f'plink.{ancestry}.clumped')
            upload(f'plink.{ancestry}.clumped', f'{outdir}/plink.{ancestry}.clumped')


def fix_clump(sp2):
    """
    Given an SP2 series value of either NONE or comma-separated list of
    rsID(filenum) strings, return an array of rsID.
    """
    if sp2 == 'NONE':
        return []

    return [s.split('(', maxsplit=1)[0] for s in sp2.split(',')]


def load_plink(clump_file):
    """
    Returns two series: top SNPs and clumped SNPs
    """
    df = pd.read_csv(clump_file, sep='\s+', header=0)
    df = df[['SNP', 'SP2']]

    # split clumped variants, explode into snp -> clumped_snp
    df['SP2'] = df['SP2'].map(fix_clump)

    # top SNP series and clumped SNP series
    return df


def build_graph(df):
    """
    Given a DataFrame of SNP -> SP2, build a sparse matrix of connections
    between them to determine which clumps each SNP belongs to.

    Find the connected components between them and create a new dataframe
    of SNP -> clump ID.

    Join this with the original frame to remove clumped SNPs and only keep
    the top SNPs from the original frame.
    """
    snps = set()

    # build a set of all the unique snp ids
    for i, (snp, sp2) in df.iterrows():
        snps.add(snp)
        snps.update(sp2)

    # convert to a list for indexing
    labels = list(snps)

    # create the matrix
    m = lil_matrix((len(labels), len(labels)), dtype=int)

    # build a vocabulary of snp -> index for fast lookup
    voc = {snp:i for i, snp in enumerate(labels)}

    # build the square-matrix of all the connections
    for i, (snp, sp2) in df.iterrows():
        for s in sp2:
            m[voc[snp], voc[s]] = 1
            m[voc[s], voc[snp]] = 1

    # find all the connected snps
    n, clumps = connected_components(m)

    # connected_components begins labels at 0, we want to start at 1
    clumps = [n + 1 for n in clumps]

    # build a dataframe
    return pd.DataFrame({'dbSNP': labels, 'clump': clumps})


def merge_results():
    """
    Load all the clumped results together and merge them.
    """
    plink_files = glob.glob('plink.*.clumped')
    if len(plink_files) == 0:
        return pd.DataFrame()

    # join all the ancestries together
    df = pd.concat(load_plink(o) for o in plink_files)

    # build and process the connected graph for the clumps
    return build_graph(df)


def clump_ranges(df):
    """
    Returns a dictionary of every clump ID mapped to a tuple of (min, max)
    position.
    """
    clumps = {}

    for i, row in df.iterrows():
        clump, pos = row['clump'], row['position']
        r = clumps.get(clump)

        if r is None:
            clumps[clump] = (pos, pos + 1)
        else:
            clumps[clump] = (min(r[0], pos), max(r[1], pos + 1))

    return clumps


def concat_rare(clumped, rare):
    """
    Append rare variants that aren't within a range of any clumped.
    """
    ranges = list(clumped[['chromosome', 'clumpStart', 'clumpEnd']].itertuples(index=False, name=None))

    # returns True if position is within any of the ranges
    def is_clumped(chromosome, position):
        return any(map(lambda r: r[0] == chromosome and r[1] <= position < r[2], ranges))

    # find all rare variants not within any range
    outside = rare[rare.apply(lambda row: not is_clumped(row.chromosome, row.position), axis=1)]

    # for the 'outside' variants, set their range to 1 bp
    outside['clumpStart'] = outside['position'].copy()
    outside['clumpEnd'] = outside['position'].copy() + 1

    rare_clump_id = clumped['clump'].max() + 1
    last_clump_id = rare_clump_id + len(outside.index)

    # add clump id starting from clumped['clump'].max() + 1
    outside['clump'] = list(range(rare_clump_id, last_clump_id))

    outside['freqType'] = 'rare'

    # only concat if there is something to append
    if not outside.empty:
        return pd.concat([clumped, outside])

    return clumped


def cleanup():
    for fn in glob.glob('plink.*'):
        os.remove(fn)
    for fn in glob.glob('part-*'):
        os.remove(fn)
    os.remove('snps.assoc')


def main():
    opts = argparse.ArgumentParser()
    opts.add_argument('--phenotype', type=str, required=True)
    opts.add_argument('--ancestry', type=str, required=True)
    opts.add_argument('--meta-type', type=str, required=True)
    opts.add_argument('--param-type', type=str, required=True)

    # parse command line
    args = opts.parse_args()
    params = params_by_type[args.param_type]

    # source data and output location
    if args.ancestry == 'TE':
        srcdir = f'{s3_in}/out/metaanalysis/{args.meta_type}/trans-ethnic/{args.phenotype}'
        plinkdir = f'{s3_out}/out/metaanalysis/{args.meta_type}/staging/plink/{args.param_type}/{args.phenotype}'
        outdir = f'{s3_out}/out/metaanalysis/{args.meta_type}/staging/clumped/{args.param_type}/{args.phenotype}'
    else:
        srcdir = f'{s3_in}/out/metaanalysis/{args.meta_type}/ancestry-specific/{args.phenotype}/ancestry={args.ancestry}'
        plinkdir = f'{s3_out}/out/metaanalysis/{args.meta_type}/staging/ancestry-plink/{args.param_type}/{args.phenotype}/{args.ancestry}'
        outdir = f'{s3_out}/out/metaanalysis/{args.meta_type}/staging/ancestry-clumped/{args.param_type}/{args.phenotype}/ancestry={args.ancestry}'
    ancestries = G1000_ANCESTRIES_BY_PORTAL_ANCESTRIES[args.ancestry]

    # download and read the meta-analysis results
    df = load_bottom_line(f'{srcdir}/', params)

    # if there are no associations, just stop
    if df.empty:
        return

    # load the SNPs file
    snps = pd.read_csv(f'{CLUMPING_ROOT}/snps.csv', sep='\t', header=0)

    # join to get dbSNP for each variant, ignore variants w/o a rsID
    df = df.merge(snps, on='varId', how='left')

    # separate common (has dbSNP) and rare associations
    common = df[df['dbSNP'].notna()]
    rare = df[df['dbSNP'].isna() & (df['pValue'] < params['p1'])]

    # join and write out the assoc file for plink
    build_assoc_file('snps.assoc', common)
    run_plink('snps.assoc', plinkdir, ancestries, params)

    # get the final output of top and clumped SNPs (clump ID, SNP)
    clumped = merge_results()
    if clumped.empty:
        cleanup()
        return

    # get the variant ID and bottom-line columns back
    clumped = clumped.merge(snps, on='dbSNP')
    clumped = clumped.merge(df, on='varId')

    # get the min, max positions of every clump
    ranges = clump_ranges(clumped)

    # define clump range columns
    clumped['clumpStart'] = clumped['clump'].apply(lambda i: ranges[i][0])
    clumped['clumpEnd'] = clumped['clump'].apply(lambda i: ranges[i][1])

    # frequency type
    clumped['freqType'] = 'common'

    # add rare variants that do not overlap a clumped range
    clumped = concat_rare(clumped, rare)

    # make sure the clump ID is an integer
    clumped = clumped.astype({'clump': np.int32})

    # finally, append the phenotype, meta type, and param type to data
    clumped['phenotype'] = args.phenotype
    clumped['metaType'] = args.meta_type
    clumped['paramType'] = args.param_type

    # filter out only the data needed for later joins
    clumped = clumped[['varId', 'phenotype', 'metaType', 'paramType', 'freqType', 'clump', 'clumpStart', 'clumpEnd']]

    # sort by clump for easy debugging in S3
    clumped = clumped.sort_values('clump')

    # As a final step drop duplicates
    clumped = clumped.drop_duplicates()

    # write the output file and upload it
    clumped.to_json('variants.json', orient='records', lines=True)
    upload('variants.json', outdir)

    cleanup()
    os.remove('variants.json')


if __name__ == '__main__':
    main()
