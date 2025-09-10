import argparse
import glob
import gzip
import os
import shutil
import subprocess
from typing import Dict, List
import heapq


s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']


def download_data(list_group: str) -> None:
    subprocess.check_call(f'aws s3 cp {s3_in}/out/pigean/gene_lists/{list_group}/ list_files/ --recursive', shell=True)


def get_file_list() -> List:
    list_files = glob.glob('list_files/*.list')
    if len(list_files) > 1:
        raise Exception(f'ERROR: Only one .list file allowed for list_group, {len(list_files)} found.')

    file_list = []
    with open(list_files[0], 'r') as f:
        for line in f:
            set_name, set_file = line.strip().split(':')
            file_name = set_file.split('/')[-1]
            file_list.append((set_name, file_name))
    return file_list


def get_gene_sets(file: str) -> Dict:
    gene_set_to_gene = {}
    with gzip.open(f'list_files/{file}', 'rt') as f:
        for line in f:
            set_name, gene_str = line.strip().split('\t', 1)
            genes = gene_str.split('\t')
            gene_set_to_gene[set_name] = set(genes)
    return gene_set_to_gene


def get_overlaps(file_list: List) -> Dict:
    overlap_gene_sets = {}
    for main_idx in range(len(file_list)):
        main_name, main_file = file_list[main_idx]
        main_sets = get_gene_sets(main_file)
        for other_idx in range(main_idx + 1, len(file_list)):
            other_name, other_file = file_list[other_idx]
            print(main_name, other_name)
            min_overlap = 10
            other_sets = get_gene_sets(other_file)
            for main_set_name, main_set in main_sets.items():
                if len(main_set) >= min_overlap:
                    for other_set_name, other_set in other_sets.items():
                        overlap = main_set & other_set
                        if len(overlap) >= min_overlap:
                            if (main_name, other_name) not in overlap_gene_sets:
                                overlap_gene_sets[(main_name, other_name)] = []
                            heapq.heappush(overlap_gene_sets[(main_name, other_name)], (len(overlap), overlap, main_set_name, other_set_name))
                            if len(overlap_gene_sets[(main_name, other_name)]) > 5000:
                                gene_set_size, _, _, _ = heapq.heappop(overlap_gene_sets[(main_name, other_name)])
                                if gene_set_size > min_overlap:
                                    min_overlap = gene_set_size
                                    print(min_overlap)
    return overlap_gene_sets


def upload_data(list_group: str, overlap_gene_sets: Dict) -> None:
    os.makedirs('overlap_files', exist_ok=True)
    with open(f'overlap_files/{list_group}_overlap.gene_sets.list', 'w') as f_list:
        for (main_name, other_name), overlap_sets in overlap_gene_sets.items():
            file_name = f'{main_name}.x.{other_name}.gmt.gz'
            f_list.write(f'{main_name}_x_{other_name}:/mnt/var/pigean/{list_group}_overlap/{file_name}\n')
            with gzip.open(f'overlap_files/{file_name}', 'wt') as f:
                for (_, gene_set, main_set_name, other_set_name) in overlap_sets:
                    set_name = f'{main_set_name}_{other_set_name}'
                    gene_str = '\t'.join(gene_set)
                    f.write(f'{set_name}\t{gene_str}\n')
    #subprocess.check_call(f'aws s3 cp overlap_files/ {s3_in}/out/pigean/gene_lists/{list_group}_overlap/ --recursive', shell=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--list-group', default=None, required=True, type=str, help="group e.g. cfde")
    args = parser.parse_args()
    download_data(args.list_group)
    file_list = get_file_list()
    overlap_gene_sets = get_overlaps(file_list)
    upload_data(args.list_group, overlap_gene_sets)
    # shutil.rmtree('list_files')
    # shutil.rmtree('overlap_files')


if __name__ == '__main__':
    main()
