#!/usr/bin/python3

import argparse
import math
import os
import subprocess
import pandas as pd
import statsmodels.api as sm

# color map
COLORS = ['#08306b', '#41ab5d', '#000000', '#f16913', '#3f007d', '#cb181d']

# sorted chromosomes
CHROMOSOMES = [str(i + 1) for i in range(22)] + ["X", "Y"]

# length of each chromosome
CHROMOSOME_LEN = {
    "1": 247249719,
    "2": 242951149,
    "3": 199501827,
    "4": 191273063,
    "5": 180857866,
    "6": 170899992,
    "7": 158821424,
    "8": 146274826,
    "9": 140273252,
    "10": 135374737,
    "11": 134452384,
    "12": 132349534,
    "13": 114142980,
    "14": 106368585,
    "15": 100338915,
    "16": 88827254,
    "17": 78774742,
    "18": 76117153,
    "19": 63811651,
    "20": 62435964,
    "21": 46944323,
    "22": 49691432,
    "X": 154913754,
    "Y": 57772954,
}

# start position and color of each chromosome
CHROMOSOME_START = {}
CHROMOSOME_COLOR = {}
CHROMOSOME_XTICK = {}

# map chromosome names
CHROMOSOME_MAP = {**{c: c for c in CHROMOSOMES}, "23": "X", "24": "Y"}


def build_chromosome_map():
    """
    Build the constant maps for chromosome start position and color.
    """
    pos = 0
    for i, chrom in enumerate(CHROMOSOMES):
        CHROMOSOME_START[chrom] = pos
        CHROMOSOME_COLOR[chrom] = COLORS[i % len(COLORS)]
        CHROMOSOME_XTICK[chrom] = CHROMOSOME_START[chrom] + CHROMOSOME_LEN[chrom] // 2

        # advance position to next chromosome
        pos += CHROMOSOME_LEN[chrom]


def x_pos(chromosome, position):
    """
    Absolute position on the x-axis for a variant.
    """
    return CHROMOSOME_START[chromosome] + position


def main():
    """
    Arguments: srcdir outdir
    Example: out/metaanalysis/trans-ethnic/T2D phenotype/T2D
    """
    opts = argparse.ArgumentParser()
    opts.add_argument('srcdir')
    opts.add_argument('outdir')

    # parse command line arguments
    args = opts.parse_args()
    dry_run = os.getenv("JOB_DRYRUN")
    job_bucket = os.getenv("JOB_BUCKET")

    # source glob to read from and outdir to write to
    srcdir = f'{job_bucket}/{args.srcdir.strip("/")}/*.json'
    outdir = f's3://dig-bio-{"test" if dry_run else "index"}/plots/{args.outdir.strip("/")}'

    # local associations file
    associations = f'associations.json'

    # merge all sources together locally into one single file
    subprocess.check_call([
        'hadoop',
        'fs',
        '-getmerge',
        '-nl',
        '-skip-empty-file',
        srcdir,
        associations,
    ])

    # load the entire dataset into a dataframe
    df = pd.read_json(associations, lines=True)

    # make sure the chromosome column is a string type
    df['chromosome'] = df['chromosome'].astype(str)

    # calculate the -log10(p) column
    df['-log10(p)'] = df['pValue'].map(lambda p: -math.log10(p))

    # select only the chromosome, position, and -log10(p) columns
    df = df[['chromosome', 'position', '-log10(p)']]

    # remove any rows with an invalid chromosome
    df = df[df['chromosome'].map(lambda c: c in CHROMOSOMES)]

    # add a column with the x-axis coordinate for the association
    df['x'] = df.apply(lambda r: x_pos(r['chromosome'], r['position']), axis=1)

    # add a color column based on the chromosome
    df['color'] = df['chromosome'].map(lambda c: CHROMOSOME_COLOR[c])

    # plot the associations
    ax = df.plot(kind='scatter', x='x', y='-log10(p)', s=5, color=df['color'])
    ax.set_xlabel('chromosome')
    ax.set_xticks([CHROMOSOME_XTICK[c] for c in CHROMOSOMES])
    ax.set_xticklabels(CHROMOSOMES)

    # maximum position on the x-axis
    xmax = CHROMOSOME_START['Y'] + CHROMOSOME_LEN['Y']

    # significance lines
    ax.hlines(5, 0, xmax, linestyle='dashed', color='gray')
    ax.hlines(8, 0, xmax, linestyle='dashed', color='red')

    # size and save the plot
    fig = ax.get_figure()
    fig.set_size_inches(15, 8)
    fig.savefig('manhattan.png')

    # create a qq-plot of the associations
    qq = sm.qqplot(df['-log10(p)'], line='q', fit=True)
    qq.set_size_inches(15, 8)
    qq.savefig('qq.png')

    # delete the associations file as it's no longer needed
    os.remove(associations)

    # upload the manhattan plot to the bioindex
    subprocess.check_call([
        'aws',
        's3',
        'cp',
        'manhattan.png',
        f'{outdir}/manhattan.png',
    ])

    # upload the qq plot to the bioindex
    subprocess.check_call([
        'aws',
        's3',
        'cp',
        'qq.png',
        f'{outdir}/qq.png',
    ])


if __name__ == '__main__':
    build_chromosome_map()
    main()
