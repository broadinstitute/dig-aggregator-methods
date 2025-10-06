#!/usr/bin/python3
import argparse
import numpy as np
import os
import shutil
import struct
import subprocess
import gzip
from typing import Dict, Iterator, List, Tuple
from numpy import typing as npt

downloaded_files = '/mnt/var/ldsc'
g1000_files = f'{downloaded_files}/g1000'
snp_files = f'{downloaded_files}/snps'

s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']


def get_region_file(sub_region, region_name):
    file = f'{s3_in}/out/ldsc/regions/merged/{sub_region}/{region_name}/{region_name}.csv'
    region_dir = f'./{sub_region}/{region_name}/'
    subprocess.check_call(f'aws s3 cp "{file}" "{region_dir}"', shell=True)


def get_annotation_data(sub_region: str, region_name: str, file_chromosome: str) -> List[Tuple[int, int]]:
    data = []
    file = f'{sub_region}/{region_name}/{region_name}.csv'
    with open(file, 'r') as f:
        for line in f:
            chromosome, start, end = line.strip().split('\t', 2)
            if '\t' in end:
                end, _ = end.split('\t', 1)
            if chromosome == file_chromosome:
                data.append((int(start), int(end)))
    return data


def get_g1000_data(ancestry: str, chromosome: str) -> List[int]:
    data = []
    with open(f'{g1000_files}/{ancestry}/chr{chromosome}.bim', 'r') as f:
        for line in f:
            _, _, _, position, _ = line.strip().split('\t', 4)
            data.append(int(position))
    return data


def next_range(iter_range: Iterator[Tuple[int, int]], max_val: int):
    return next(iter_range, (max_val, max_val))


def write_annot(sub_region: str, region_name: str, ancestry: str, chromosome: str, range_data: List[Tuple[int, int]], g1000_data: List) -> None:
    out_file = f'{sub_region}/{region_name}/{ancestry}/ld_score/ld.{chromosome}.annot.gz'
    with gzip.open(out_file, 'wt') as f_out:
        f_out.write('ANNOT\n')
        iter_range = iter(range_data)
        curr_start, curr_end = next_range(iter_range, g1000_data[-1])
        for curr_position in iter(g1000_data):
            while curr_end < curr_position:
                curr_start, curr_end = next_range(iter_range, g1000_data[-1])
            if curr_start < curr_position <= curr_end:
                f_out.write('1\n')
            else:
                f_out.write('0\n')


def make_annot(sub_region: str, region_name: str, ancestry: str, chromosome: str) -> None:
    range_data = get_annotation_data(sub_region, region_name, chromosome)
    g1000_data = get_g1000_data(ancestry, chromosome)
    write_annot(sub_region, region_name, ancestry, chromosome, range_data, g1000_data)


def get_bim_data(ancestry: str, chromosome: str) -> Tuple[List[int], List[float], List[Tuple[str, int]]]:
    hm3_out = []
    all_cm = []
    rsids = []
    hm_set = set()
    with open(f'{snp_files}/hm.{chromosome}.snp', 'r') as f:
        for line in f.readlines():
            hm_set |= {line.strip()}
    with open(f'{g1000_files}/{ancestry}/chr{chromosome}.bim', 'r') as f:
        for idx, line in enumerate(f.readlines()):
            split_line = line.strip().split('\t')
            rsid = split_line[1]
            all_cm.append(float(split_line[2]))
            if rsid in hm_set:
                hm3_out.append(idx)
                rsids.append((rsid, int(split_line[3])))
    return hm3_out, all_cm, rsids


def get_annotation(sub_region: str, region_name: str, ancestry: str, chromosome: str) -> List[int]:
    annot_file = f'{sub_region}/{region_name}/{ancestry}/ld_score/ld.{chromosome}.annot.gz'
    with gzip.open(annot_file, 'rt') as f:
        f.readline() # header
        return [idx for idx, line in enumerate(f.readlines()) if line == '1\n']


def write_output(sub_region: str, region_name: str, ancestry: str, chromosome: str, l2s: List[float], rsids: List[Tuple[str, int]], M: int, M_5: int) -> None:
    os.makedirs(f'{sub_region}/{region_name}/{ancestry}/ld_score/', exist_ok=True)
    with gzip.open(f'{sub_region}/{region_name}/{ancestry}/ld_score/ld.{chromosome}.l2.ldscore.gz', 'w') as f:
        f.write(b'CHR\tSNP\tBP\tL2\n')
        for (rsid, bp), l2 in zip(rsids, l2s):
            f.write(f'{chromosome}\t{rsid}\t{bp}\t{round(l2, 3)}\n'.encode())
    with open(f'{sub_region}/{region_name}/{ancestry}/ld_score/ld.{chromosome}.l2.M', 'w') as f:
        f.write(f'{M}\n')
    with open(f'{sub_region}/{region_name}/{ancestry}/ld_score/ld.{chromosome}.l2.M_5_50', 'w') as f:
        f.write(f'{M_5}\n')


def get_dimensions(ancestry: str, chromosome: str) -> Tuple[int, int]:
    with open(f'{g1000_files}/{ancestry}/chr{chromosome}.fam', 'r') as f:
        num_people = len(f.readlines())

    extra_columns = (4 - num_people % 4) if num_people % 4 != 0 else 0
    bed_width = num_people + extra_columns

    return bed_width, num_people


decode = {'01': 1, '11': 2, '00': 0, '10':9}
decode_full = {}
for i in range(256):
    z = format(i, '08b')[::-1]
    y = [decode[z[i:i+2]] for i in range(0, len(z), 2)]
    decode_full[i] = y

def get_X(ancestry:str, chromosome: str, bed_width: int, idxs: List[int]) -> npt.NDArray:
    with open(f'{g1000_files}/{ancestry}/chr{chromosome}.bed', 'rb') as fh:
        bed = fh.read()
    bf = '<' + 'B' * (bed_width // 4)
    return np.array(
        [[b for a in struct.unpack_from(bf, bed, 3 + idx * (bed_width // 4)) for b in decode_full[a]] for idx in idxs]
    )


def normalize_X(X: npt.NDArray) -> npt.NDArray:
    mean = np.mean(X, 1, keepdims=True)
    std = np.std(X, 1, keepdims=True)
    ones_array = np.ones((1, X.shape[1]))
    return (X - mean.dot(ones_array)) / std.dot(ones_array)


def get_LR(x_cm: List[float], y_cm: List[float], window_cm: float=1.0) -> List[Tuple[int, int]]:
    lr = []
    idx = left_idx = right_idx = 0
    while idx < len(x_cm):
        while right_idx < len(y_cm) and y_cm[right_idx] - x_cm[idx] <= window_cm:
            right_idx += 1
        while left_idx < len(y_cm) and x_cm[idx] - y_cm[left_idx] > window_cm:
            left_idx += 1
        lr.append((left_idx, right_idx - 1))
        idx += 1
    return lr


def make_ld(sub_region: str, region_name: str, ancestry: str, chromosome: str) -> None:
    bed_width, num_people = get_dimensions(ancestry, chromosome)
    hm3_idxs, all_cm, rsids = get_bim_data(ancestry, chromosome)
    annot = get_annotation(sub_region, region_name, ancestry, chromosome)

    if len(annot) > 0:
        X = get_X(ancestry, chromosome, bed_width, hm3_idxs)[:, :num_people]
        Y = get_X(ancestry, chromosome, bed_width, annot)[:, :num_people]

        # Filter out fully heterogeneous SNPs
        X_filter = np.sum(X == 1, 1) != num_people
        if sum(X_filter) < X.shape[0]:
            raise Exception('X filter cannot be triggered')

        Y_filter = np.sum(Y == 1, 1) != num_people
        annot = [a for i, a in enumerate(annot) if Y_filter[i]]
        Y = Y[Y_filter, :]

        # This is used for the M_5_50 calculation
        AF = np.sum(Y, 1) / 2 / num_people
        MAF = np.minimum(AF, np.ones(Y.shape[0]) - AF)
        M = Y.shape[0]
        M_5 = np.sum(MAF > 0.05)

        X = normalize_X(X)
        Y = normalize_X(Y)

        # This gets left-right bounds for matrix Y per hapmap3 SNP in the g1000 dataset
        x_cm = np.array(all_cm)[hm3_idxs]
        y_cm = np.array(all_cm)[annot]
        lr = get_LR(x_cm, y_cm)

        l2s = []
        for i, (left_idx, right_idx) in enumerate(lr):
            # LD Score calculation for SNP i
            v = Y[left_idx:right_idx + 1, :].dot(X[i, :]) / num_people
            # L2 adjustment
            l2s.append(((num_people - 1) * v.dot(v) - len(v)) / (num_people - 2))
    else:
        l2s = [0.0] * len(hm3_idxs)
        M = 0
        M_5 = 0

    write_output(sub_region, region_name, ancestry, chromosome, l2s, rsids, M, M_5)


def upload_and_remove_files(sub_region, region_name, ancestry):
    s3_dir = f'{s3_out}/out/ldsc/regions/ld_score/ancestry={ancestry}/{sub_region}/{region_name}/'
    path = f'{sub_region}/{region_name}/{ancestry}'
    subprocess.check_call(f'zip -j "{path}"/ld_score.zip "{path}"/ld_score/*', shell=True)
    subprocess.check_call(f'aws s3 cp "{path}"/ld_score.zip "{s3_dir}"', shell=True)
    shutil.rmtree(path)


def run_ancestry(sub_region, region_name, ancestry):
    os.makedirs(f'./{sub_region}/{region_name}/{ancestry}/ld_score', exist_ok=True)
    for chromosome in range(1, 23):
        make_annot(sub_region, region_name, ancestry, str(chromosome))
        make_ld(sub_region, region_name, ancestry, str(chromosome))
    upload_and_remove_files(sub_region, region_name, ancestry)


def run(sub_region, ancestries, region_name):
    get_region_file(sub_region, region_name)
    for ancestry in ancestries:
        run_ancestry(sub_region, region_name, ancestry)
    shutil.rmtree(f'./{sub_region}/{region_name}')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--sub-region', default=None, required=True, type=str,
                        help="Merge sub region.")
    parser.add_argument('--region-name', default=None, required=True, type=str,
                        help="Merge region name.")
    parser.add_argument('--ancestries', default=None, required=True, type=str,
                        help="All g1000 ancestries (e.g. EUR) to run.")
    args = parser.parse_args()
    sub_region = args.sub_region
    region_name = args.region_name
    ancestries = args.ancestries.split(',')
    run(sub_region, ancestries, region_name)


if __name__ == '__main__':
    main()
