#!/usr/bin/python3
import argparse
import numpy as np
import os
import re
import subprocess
import shutil


s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']

# Lifted from factor matrix code
def loadings_to_probabilities(
        W,
        alpha=0.5,
        log1p=True,
        max_iter=200,
        tol=1e-6,
        min_var=1e-6,
        eps=1e-12,
        return_details=False,
):
    """
    Convert a gene x factor loading matrix to per-factor probabilities in [0,1],
    penalizing genes that load broadly across many factors.

    P[g,k] = r[g,k] * ((1 - alpha) + alpha * S[g])

    where:
      - r[g,k] is the posterior that gene g belongs to factor k from a
        2-component Gaussian mixture on z = log1p(W[:,k]) (or raw values if log1p=False).
      - S[g] is gene-wise specificity = 1 - entropy(q_g)/log(K), with
        q_gk = (W[g,k] + eps) / sum_j (W[g,j] + eps).

    Parameters
    ----------
    W : np.ndarray, shape (G, K)
        Nonnegative loadings (genes x factors).
    alpha : float in [0,1], default 0.5
        Specificity weight. 0 disables the penalty; 1 gives full weight.
    log1p : bool, default True
        Fit mixtures on log1p(loadings) for stability.
    max_iter : int, default 200
        Max EM iterations per factor.
    tol : float, default 1e-6
        Convergence tolerance for EM parameter updates.
    min_var : float, default 1e-6
        Minimum variance for mixture components (in chosen space).
    eps : float, default 1e-12
        Small constant for numerical stability and clipping.
    return_details : bool, default False
        If True, also return (r, S).

    Returns
    -------
    P : np.ndarray, shape (G, K)
        Probabilities in [0,1].
    r : np.ndarray, shape (G, K), optional
        Per-factor mixture posteriors before specificity (if return_details=True).
    S : np.ndarray, shape (G,), optional
        Gene-wise specificity in [0,1] (if return_details=True).
    """
    W = np.asarray(W, dtype=float)
    if W.ndim != 2:
        raise ValueError("W must be a 2D array of shape (genes, factors).")
    if np.any(W < 0):
        W = np.maximum(W, 0.0)

    G, K = W.shape
    if G == 0 or K == 0:
        raise ValueError("W must have positive dimensions.")

    def _gaussian_pdf(x, mu, var):
        var = max(var, 1e-12)
        coef = 1.0 / np.sqrt(2.0 * np.pi * var)
        return coef * np.exp(-0.5 * (x - mu) * (x - mu) / var)

    def _em_2comp_gaussian(z):
        z = np.asarray(z, dtype=float)
        good = np.isfinite(z)
        if good.sum() < 3:
            out = np.zeros_like(z, dtype=float)
            return out

        zg = z[good]
        if np.nanstd(zg) < 1e-12:
            out = np.zeros_like(z, dtype=float)
            out[good] = 0.0
            return out

        q25 = np.nanpercentile(zg, 25.0)
        q85 = np.nanpercentile(zg, 85.0)
        mu1 = float(q25)
        mu2 = float(q85 if q85 > q25 else q25 + 1e-3)
        var1 = float(max(np.nanvar(zg[zg <= np.nanmedian(zg)]), min_var))
        var2 = float(max(np.nanvar(zg[zg >= np.nanmedian(zg)]), min_var))
        pi2 = 0.1
        pi1 = 0.9

        for i in range(max_iter):
            n1 = _gaussian_pdf(zg, mu1, var1) * pi1
            n2 = _gaussian_pdf(zg, mu2, var2) * pi2
            denom = n1 + n2 + 1e-30
            gamma2 = n2 / denom  # resp for component 2

            Nk2 = float(np.sum(gamma2))
            Nk1 = float(np.sum(1.0 - gamma2))
            if Nk1 < 1e-8 or Nk2 < 1e-8:
                break

            mu1_new = float(np.sum((1.0 - gamma2) * zg) / max(Nk1, 1e-12))
            mu2_new = float(np.sum(gamma2 * zg) / max(Nk2, 1e-12))
            var1_new = float(np.sum((1.0 - gamma2) * (zg - mu1_new) ** 2) / max(Nk1, 1e-12))
            var2_new = float(np.sum(gamma2 * (zg - mu2_new) ** 2) / max(Nk2, 1e-12))
            var1_new = max(var1_new, min_var)
            var2_new = max(var2_new, min_var)
            pi2_new = Nk2 / zg.size
            pi1_new = 1.0 - pi2_new

            delta = max(
                abs(mu1_new - mu1),
                abs(mu2_new - mu2),
                abs(var1_new - var1),
                abs(var2_new - var2),
                abs(pi2_new - pi2),
            )
            mu1, mu2, var1, var2, pi1, pi2 = mu1_new, mu2_new, var1_new, var2_new, pi1_new, pi2_new
            if delta < tol:
                break

        signal_is_2 = mu2 >= mu1
        n1 = _gaussian_pdf(zg, mu1, var1) * pi1
        n2 = _gaussian_pdf(zg, mu2, var2) * pi2
        denom = n1 + n2 + 1e-30
        r_good = (n2 / denom) if signal_is_2 else (n1 / denom)

        out = np.zeros_like(z, dtype=float)
        out[good] = r_good
        return out

    # Step 1: per-factor posteriors r[g,k]
    r = np.zeros_like(W, dtype=float)
    for k in range(K):
        col = W[:, k]
        z = np.log1p(col) if log1p else col.copy()
        if np.allclose(z, z[0], atol=0.0):
            r[:, k] = 0.0
        else:
            r[:, k] = np.clip(_em_2comp_gaussian(z), 0.0, 1.0)

    # Step 2: gene-wise specificity S[g]
    row_sums = np.sum(W, axis=1, keepdims=True) + eps * K
    Q = (W + eps) / row_sums
    with np.errstate(divide="ignore", invalid="ignore"):
        logQ = np.log(Q)
    H = -np.sum(Q * logQ, axis=1)
    Hmax = np.log(K) if K > 1 else 1.0
    S = 1.0 - (H / Hmax)
    S = np.clip(S, 0.0, 1.0)

    # Step 3: combine
    alpha = float(max(0.0, min(1.0, alpha)))
    P = r * ((1.0 - alpha) + alpha * S[:, None])
    P = np.clip(P, eps, 1.0 - eps)

    if return_details:
        return P, r, S
    return P


def download(tissue, cell_type, dataset):
    path = f'{s3_in}/out/single_cell/staging/nmf/liger/{tissue}/{cell_type}/{dataset}/'
    cmd = ['aws', 's3', 'cp', path, 'inputs/', '--recursive']
    subprocess.check_call(cmd)


def convert_cell_loadings():
    with open(f'outputs/factor_matrix_cell_loadings.tsv', 'w') as f_out:
        with open(f'inputs/cell_factor_scores.csv', 'r') as f_in:
            factors = f_in.readline().strip().split(',')[1:]
            f_out.write('Cell\t{}\n'.format('\t'.join([f'Factor{idx + 1}' for idx in range(len(factors))])))
            for line in f_in:
                cell, data = line.split(',', 1)
                f_out.write('{}\t{}\n'.format(cell, '\t'.join(data.strip().split(','))))


def convert_gene_loadings():
    with open(f'outputs/factor_matrix_gene_loadings.tsv', 'w') as f_out:
        with open(f'inputs/factor_gene_programs.csv', 'r') as f_in:
            factors = f_in.readline().strip().split(',')[1:]
            f_out.write('Gene\t{}\n'.format('\t'.join([f'Factor{idx + 1}' for idx in range(len(factors))])))
            for line in f_in:
                f_out.write('\t'.join(line.strip().split(',')) + '\n')


def convert_gene_probabilities():
    with open(f'inputs/factor_gene_programs.csv', 'r') as f_in:
        factors = f_in.readline().strip().split(',')[1:]
        genes = []
        W = []
        for line in f_in:
            gene, data = line.strip().split(',', 1)
            W.append(list(map(float, data.split(','))))
            genes.append(gene)
        P = loadings_to_probabilities(W, alpha=0.5)
    with open(f'outputs/factor_matrix_gene_probs.tsv', 'w') as f_out:
        f_out.write('Gene\t{}\n'.format('\t'.join([f'Factor{idx + 1}' for idx in range(len(factors))])))
        for gene_idx, gene in enumerate(genes):
            f_out.write('{}\t{}\n'.format(
                gene,
                '\t'.join(map(lambda x: str(round(x, 5)), P[gene_idx]))
            ))


def get_top_genes():
    with open(f'outputs/factor_matrix_gene_loadings.tsv', 'r') as f_in:
        header = f_in.readline().strip().split('\t')[1:]
        top_genes = {f'Factor{idx + 1}': [] for idx in range(len(header))}
        for line in f_in:
            gene, data = line.strip().split('\t', 1)
            factor_data = list(map(float, data.split('\t')))
            for idx, factor_datum in enumerate(factor_data):
                top_genes[f'Factor{idx + 1}'].append((factor_datum, gene))
    return {factor: [a[1] for a in sorted(top_genes[factor], reverse=True)[:5]] for factor in top_genes}


def get_top_cells():
    with open(f'outputs/factor_matrix_cell_loadings.tsv', 'r') as f_in:
        header = f_in.readline().strip().split('\t')[1:]
        top_cells = {f'Factor{idx + 1}': [] for idx in range(len(header))}
        for line in f_in:
            cell, data = line.strip().split('\t', 1)
            factor_data = list(map(float, data.split('\t')))
            for idx, factor_datum in enumerate(factor_data):
                top_cells[f'Factor{idx + 1}'].append((factor_datum, cell))
    return {factor: [a[1] for a in sorted(top_cells[factor], reverse=True)[:5]] for factor in top_cells}


def convert_gene_programs():
    top_genes = get_top_genes()
    top_cells = get_top_cells()
    with open(f'inputs/factor_summary.csv', 'r') as f:
        importances = []
        header = f.readline()
        for line in f:
            line_dict = dict(zip(header.strip().split(','), line.strip().split(',')))
            importances.append(float(line_dict['max_gene_loading']))
    with open(f'outputs//factor_matrix_factors.tsv', 'w') as f_out:
        f_out.write('factor\texp_lambdak\ttop_genes\ttop_cells\n')
        for idx, importance in enumerate(importances):
            f_out.write('{}\t{}\t{}\t{}\n'.format(
                f'Factor{idx + 1}',
                importance,
                ','.join(top_genes[f'Factor{idx + 1}']),
                ','.join(top_cells[f'Factor{idx + 1}'])
            ))


def convert():
    os.makedirs(f'outputs', exist_ok=True)
    convert_cell_loadings()
    convert_gene_loadings()
    convert_gene_probabilities()
    convert_gene_programs()


def upload(tissue, cell_type, dataset):
    path = f'{s3_out}/out/single_cell/staging/factor_matrix/{tissue}/{cell_type}/{dataset}'
    cmd = ['aws', 's3', 'cp', f'outputs/', path, '--recursive']
    subprocess.check_call(cmd)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--tissue', default=None, required=True, type=str,
                        help="Tissue")
    parser.add_argument('--cell-type', default=None, required=True, type=str,
                        help="Cell Type")
    parser.add_argument('--dataset', default=None, required=True, type=str,
                        help="Dataset name")
    args = parser.parse_args()

    download(args.tissue, args.cell_type, args.dataset)
    convert()
    upload(args.tissue, args.cell_type, args.dataset)
    shutil.rmtree('inputs')
    shutil.rmtree('outputs')


if __name__ == '__main__':
    main()
