import argparse
import glob
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


def download(dataset):
    path = f'{s3_in}/out/single_cell/staging/liger/{dataset}/liger.zip'
    cmd = ['aws', 's3', 'cp', path, 'inputs/']
    subprocess.check_call(cmd)
    cmd = ['unzip', 'inputs/liger.zip', '-d', 'inputs/']
    subprocess.check_call(cmd)


def format_cell_type(cell_type):
    return re.sub(r'[^a-zA-Z0-9_-]', '', cell_type.replace(' ', '_').lower())


def get_cell_map():
    files = glob.glob('inputs/output/*/metadata.txt')
    cell_types = [re.findall('inputs/output/(.*)/metadata.txt', file)[0] for file in files]
    print(cell_types)
    return {format_cell_type(cell_type): cell_type for cell_type in cell_types}


def convert_cell_loadings(cell_type, cell_type_name):
    with open(f'outputs/{cell_type}/factor_matrix_cell_loadings.tsv', 'w') as f_out:
        with open(f'inputs/output/{cell_type_name}/cell_scores.tsv', 'r') as f_in:
            header = f_in.readline()
            f_out.write('cell\tindep\t{}'.format(header))
            for line in f_in:
                batch_plus_cell, data = line.split('\t', 1)
                batch, cell = batch_plus_cell.split('_', 1)  # Will have to actually do right somehow?
                f_out.write('{}\tTrue\t{}'.format(cell, data))


def convert_gene_loadings(cell_type, cell_type_name):
    with open(f'outputs/{cell_type}/factor_matrix_gene_loadings.tsv', 'w') as f_out:
        with open(f'inputs/output/{cell_type_name}/gene_loadings.tsv', 'r') as f_in:
            header = f_in.readline()
            f_out.write('gene\t{}'.format(header))
            for line in f_in:
                f_out.write(line)


def convert_gene_probabilities(cell_type, cell_type_name):
    with open(f'inputs/output/{cell_type_name}/gene_loadings.tsv', 'r') as f_in:
        factors = f_in.readline().strip().split('\t')
        genes = []
        W = []
        for line in f_in:
            gene, data = line.strip().split('\t', 1)
            W.append(list(map(float, data.split('\t'))))
            genes.append(gene)
        P = loadings_to_probabilities(W, alpha=0.5)
    with open(f'outputs/{cell_type}/factor_matrix_gene_probs.tsv', 'w') as f_out:
        f_out.write('gene\t{}\n'.format('\t'.join(factors)))
        for gene_idx, gene in enumerate(genes):
            f_out.write('{}\t{}\n'.format(
                gene,
                '\t'.join(map(lambda x: str(round(x, 5)), P[gene_idx]))
            ))


def get_top_genes(cell_type):
    with open(f'outputs/{cell_type}/factor_matrix_gene_loadings.tsv', 'r') as f_in:
        header = f_in.readline().strip().split('\t')[1:]
        top_genes = {factor: [] for factor in header}
        for line in f_in:
            gene, data = line.strip().split('\t', 1)
            factor_data = list(map(float, data.split('\t')))
            for factor, factor_datum in zip(header, factor_data):
                top_genes[factor].append((factor_datum, gene))
    return {factor: [a[1] for a in sorted(top_genes[factor], reverse=True)[:5]] for factor in top_genes}


def get_top_cells(cell_type):
    with open(f'outputs/{cell_type}/factor_matrix_cell_loadings.tsv', 'r') as f_in:
        header = f_in.readline().strip().split('\t')[2:]
        top_cells = {factor: [] for factor in header}
        for line in f_in:
            cell, indep, data = line.strip().split('\t', 2)
            factor_data = list(map(float, data.split('\t')))
            for factor, factor_datum in zip(header, factor_data):
                top_cells[factor].append((factor_datum, cell))
    return {factor: [a[1] for a in sorted(top_cells[factor], reverse=True)[:5]] for factor in top_cells}


def convert_gene_programs(cell_type, cell_type_name):
    top_genes = get_top_genes(cell_type)
    top_cells = get_top_cells(cell_type)
    with open(f'inputs/output/{cell_type_name}/gene_programs.txt', 'r') as f:
        factors = f.readline().strip().split('\t')
    with open(f'outputs/{cell_type}/factor_matrix_factors.tsv', 'w') as f_out:
        f_out.write('factor_index\texp_lambdak\ttop_genes\ttop_cells\n')
        for factor in factors:
            f_out.write('{}\t1.0\t{}\t{}\n'.format(
                re.findall(r'Factor_([0-9]*)', factor)[0],
                ','.join(top_genes[factor]),
                ','.join(top_cells[factor])
            ))


def convert(cell_type, cell_type_name):
    os.makedirs(f'outputs/{cell_type}/', exist_ok=True)
    convert_cell_loadings(cell_type, cell_type_name)
    convert_gene_loadings(cell_type, cell_type_name)
    convert_gene_probabilities(cell_type, cell_type_name)
    convert_gene_programs(cell_type, cell_type_name)


def upload(dataset, cell_type, model):
    path = f'{s3_out}/out/single_cell/staging/factor_matrix/{dataset}/{cell_type}/{model}'
    cmd = ['aws', 's3', 'cp', f'outputs/{cell_type}/', path, '--recursive']
    subprocess.check_call(cmd)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset', default=None, required=True, type=str,
                        help="Dataset name")
    parser.add_argument('--model', default=None, required=True, type=str,
                        help="Model")
    args = parser.parse_args()

    download(args.dataset)
    cell_map = get_cell_map()
    for cell_type, cell_type_name in cell_map.items():
        convert(cell_type, cell_type_name)
        upload(args.dataset, cell_type, args.model)
    shutil.rmtree('inputs')
    shutil.rmtree('outputs')


if __name__ == '__main__':
    main()
