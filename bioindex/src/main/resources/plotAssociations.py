#!/usr/bin/python3
import argparse
import math
import os
import shutil
import subprocess
import matplotlib.pyplot as plt
import numpy as np
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
CHROMOSOME_FRAME = {}
CHROMOSOME_XTICK = {}

# map chromosome names
CHROMOSOME_MAP = {**{c: c for c in CHROMOSOMES}, "23": "X", "24": "Y"}


def build_chromosome_map():
    """
    Build the constant maps for chromosome start position and color.
    """
    pos = 0
    for i, chrom in enumerate(CHROMOSOMES):
        CHROMOSOME_FRAME[chrom] = {'chromosome': chrom, 'x': pos, 'color': COLORS[i % len(COLORS)]}
        CHROMOSOME_XTICK[chrom] = pos + CHROMOSOME_LEN[chrom] // 2

        # advance position to next chromosome
        pos += CHROMOSOME_LEN[chrom]


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

    # source glob to read from and outdir to write to
    srcdir = f's3://dig-analysis-data/{args.srcdir.strip("/")}/'
    outdir = f's3://dig-bio-index/plot/{args.outdir.strip("/")}'

    # NOTE: There is currently a bug in pandas where read_json doesn't properly
    #       reduce memory usage needed with chunksize. Once this is fixed, this
    #       parameter can be used with a `hadoop fs -getmerge` of the files.
    #
    #       Until then, it's easier to just copy all the files locally and read
    #       them one at a time. This treats the directory like a reader instead
    #       of a single file.

    parts_dir = 'associations'
    subprocess.check_call(['aws', 's3', 'cp', srcdir, f'{parts_dir}/', '--recursive'])
    parts = sorted([f for f in os.listdir(parts_dir) if f.endswith('.json')])

    # create a frame for the chromosome start positions
    chrom_pos = pd.DataFrame.from_dict(CHROMOSOME_FRAME, orient='index') \
        .set_index('chromosome')

    # collect all p-values together into a single frame across all parts
    p_values = pd.Series(dtype=np.float64)

    # create the manhattan plot
    fig, ax = plt.subplots()

    # setup the axes
    ax.set_ylabel('-log10(p)')
    ax.set_xlabel('chromosome')
    ax.set_xticks([CHROMOSOME_XTICK[c] for c in CHROMOSOMES])
    ax.set_xticklabels(CHROMOSOMES)

    # maximum position on the x-axis
    xmax = CHROMOSOME_FRAME['Y']['x'] + CHROMOSOME_LEN['Y']

    # significance lines
    ax.hlines(5, 0, xmax, linestyle='dashed', color='gray')
    ax.hlines(8, 0, xmax, linestyle='dashed', color='red')

    # plot all the chunks
    for i, part in enumerate(parts, start=1):
        print(f'Plotting {part} ({i}/{len(parts)})...')

        # read the part into memory; remove invalid p-value records
        df = pd.read_json(f'{parts_dir}/{part}', lines=True)
        df = df[(df['pValue'] > 0) & (df['pValue'] <= 1)]

        # create the -log10(p) column
        df['y'] = df['pValue'].map(lambda p: -math.log10(p))
        df['chromosome'] = df['chromosome'].astype(str)

        # calculate the x-position
        df = df.merge(chrom_pos, on='chromosome')
        df['x'] += df['position']

        # remove extraneous columns; plot the associations
        df = df[['x', 'y', 'color']]
        ax.scatter(df['x'], df['y'], s=5, color=df['color'])

        # append just the p-value to the qq dataframe
        p_values = p_values.append(df['y'].copy())

    # save the manhattan plot
    fig.set_size_inches(15, 8)
    fig.savefig('manhattan.png')

    # create the qq plot
    fig, ax = plt.subplots()

    # calculate the expected, uniformly distributed p-values
    n = len(p_values)
    expected = pd.Series(np.arange(0, n, 1)) \
        .map(lambda i: -math.log10(float(n - i) / n))

    # build the qq plot
    pp = sm.ProbPlot(p_values.sort_values())
    print(f'Plotting QQ...')

    # plot and save it
    fig = pp.qqplot(other=expected, line='r', xlabel='expected -log10(p)', ylabel='-log10(p)')
    fig.set_size_inches(15, 8)
    fig.savefig('qq.png')

    # upload the plots to the bioindex
    subprocess.check_call(['aws', 's3', 'cp', 'manhattan.png', f'{outdir}/manhattan.png'])
    subprocess.check_call(['aws', 's3', 'cp', 'qq.png', f'{outdir}/qq.png'])

    # delete part files to make room for other plots
    shutil.rmtree(parts_dir, ignore_errors=True)


if __name__ == '__main__':
    build_chromosome_map()
    main()
