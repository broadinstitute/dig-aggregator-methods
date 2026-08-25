#!/usr/bin/python3
import argparse
import itertools
import os
import shutil
import subprocess
from datetime import datetime

import anndata as ad
import numpy as np
import pandas as pd
from scipy.optimize import linear_sum_assignment

downloaded_files = '/mnt/var/single_cell'
s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']

K_GRID = range(10, 25, 2)
N_REPS = 10
FINAL_SEED = 1
N_GENES = 50
MAX_CELLS_PER_TYPE = 50000
MIN_CELLS_PER_TYPE = 500
DOWNSAMPLE_SEED = 1


def download_data(dataset, cell_type):
    path = f'{s3_in}/out/single_cell/staging/h5ad/{dataset}/{cell_type}/data.h5ad'
    subprocess.check_call(['aws', 's3', 'cp', path, 'inputs/data.h5ad'])


def downsample_h5ad(in_path, out_path):
    adata = ad.read_h5ad(in_path)
    rng = np.random.default_rng(DOWNSAMPLE_SEED)
    keep = rng.choice(adata.n_obs, size=min(adata.n_obs, MAX_CELLS_PER_TYPE), replace=False)
    adata[np.sort(keep)].copy().write_h5ad(out_path)
    return adata.n_obs


def run_inmf(out_dir, k, seed):
    cmd = [
        'Rscript', f'{downloaded_files}/run_inmf.R',
        'inputs/data_downsampled.h5ad',
        out_dir,
        str(k),
        str(seed)
    ]
    subprocess.check_call(cmd)


def load_W(run_dir):
    return pd.read_csv(os.path.join(run_dir, 'W.tsv'), sep='\t', index_col=0)


def match_factors(W1, W2):
    genes = W1.index.intersection(W2.index)
    a = W1.loc[genes].to_numpy()
    b = W2.loc[genes].to_numpy()
    corr = np.corrcoef(a.T, b.T)
    sim = corr[:a.shape[1], a.shape[1]:]
    sim = np.nan_to_num(sim, nan=0.0, posinf=0.0, neginf=0.0)
    sim = np.clip(sim, 0.0, 1.0)
    row_ind, col_ind = linear_sum_assignment(1.0 - sim)
    return float(sim[row_ind, col_ind].mean())


def stability_for_k(k):
    Ws = []
    i = 1
    while len(Ws) < N_REPS:
        run_dir = f'runs_tmp/k{k}_rep{i}'
        try:
            run_inmf(run_dir, k, i)
            Ws.append(load_W(run_dir))
            shutil.rmtree(run_dir)
        except:
            print(f'failed on seed {i}')
        i += 1
    sims = [match_factors(Ws[i], Ws[j]) for i, j in itertools.combinations(range(N_REPS), 2)]
    return float(np.mean(sims))


def find_best_k():
    stability = {k: stability_for_k(k) for k in K_GRID}
    best_k = max(stability, key=stability.get)
    return best_k, stability


def write_outputs(run_dir, cell_type, stability):
    W = pd.read_csv(os.path.join(run_dir, 'W.tsv'), sep='\t', index_col=0)
    H_norm = pd.read_csv(os.path.join(run_dir, 'H_norm.tsv'), sep='\t', index_col=0)

    W.to_csv('outputs/gene_loadings.tsv', sep='\t')
    H_norm.to_csv('outputs/cell_scores.tsv', sep='\t')

    pd.Series(stability, name='stability').rename_axis('k').to_csv('outputs/stability_by_k.tsv', sep='\t')

    programs = {col: W[col].sort_values(ascending=False).index[:N_GENES].tolist() for col in W.columns}
    pd.DataFrame(programs).to_csv('outputs/gene_programs.txt', sep='\t', index=False)

    importance = pd.Series(
        {col: np.linalg.norm(W[col]) * np.linalg.norm(H_norm[col]) for col in W.columns},
        name='factor_importance',
    )
    importance.to_csv('outputs/factor_importance.txt', sep='\t', header=False)

    pd.DataFrame([{
        'cell_type': cell_type,
        'k': W.shape[1],
        'method': 'LIGER_iNMF',
        'timestamp': datetime.now().isoformat(),
    }]).to_csv('outputs/metadata.txt', sep='\t', index=False)

    os.makedirs('outputs/dataset_specific', exist_ok=True)
    for fname in os.listdir(run_dir):
        if fname.endswith('_V.tsv'):
            shutil.copy(os.path.join(run_dir, fname), os.path.join('outputs/dataset_specific', fname))


def upload_data(dataset, cell_type):
    path = f'{s3_out}/out/single_cell/staging/liger/{dataset}/{cell_type}/'
    subprocess.check_call(['aws', 's3', 'cp', 'outputs/', path, '--recursive'])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset')
    parser.add_argument('--cell-type')
    args = parser.parse_args()

    download_data(args.dataset, args.cell_type)
    num_cells = downsample_h5ad('inputs/data.h5ad', 'inputs/data_downsampled.h5ad')
    if num_cells >= MIN_CELLS_PER_TYPE:
        os.remove('inputs/data.h5ad')
        os.makedirs('outputs', exist_ok=True)

        best_k, stability = find_best_k()
        run_inmf('runs_tmp/final', best_k, FINAL_SEED)
        write_outputs('runs_tmp/final', args.cell_type, stability)
        shutil.rmtree('runs_tmp')

        upload_data(args.dataset, args.cell_type)
        shutil.rmtree('outputs')
    shutil.rmtree('inputs')


if __name__ == '__main__':
    main()
