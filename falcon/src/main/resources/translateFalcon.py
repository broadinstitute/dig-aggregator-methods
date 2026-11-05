#!/usr/bin/python3
import argparse
import os
import shutil
import subprocess

s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']

CHROMOSOMES = [str(i) for i in range(1, 23)]


def download_data(phenotype):
    file_path = f'{s3_in}/out/falcon/staging/falcon/{phenotype}/'
    subprocess.check_call(['aws', 's3', 'cp', file_path, 'inputs/', '--recursive'])


def upload_data(phenotype, data_type):
    file_path = f'{s3_out}/out/falcon/{data_type}/{phenotype}/'
    subprocess.check_call(['zstd', f'outputs/{data_type}.json'])
    subprocess.check_call(['aws', 's3', 'cp', f'outputs/{data_type}.json.zst', file_path])
    success(file_path)


def make_bool(value):
    return 'true' if value == 'True' else 'false'


def make_str_option(value):
    return f'"{value}"' if value != 'None' else 'null'


def translate_genes(json_line, phenotype):
    abs_pos = int(json_line['CHR']) * 1000000000 + int(json_line['START'])
    return f'{{"GENE": "{json_line["GENE"]}", ' \
           f'"PIP": {json_line["PROBABILITY"]}, ' \
           f'"GENE_R": {json_line["GENE_R"]}, ' \
           f'"GENE_STAT": {make_bool(json_line["GENE_STAT"])}, ' \
           f'"P_WINDOW": {json_line["WINDOW"]}, ' \
           f'"P_DECAY": {json_line["DECAY"]}, ' \
           f'"WINDOW_R": {json_line["WINDOW_R"]}, ' \
           f'"WINDOW_STAT": {make_bool(json_line["WINDOW_STAT"])}, ' \
           f'"P_BETA": {json_line["BETA"]}, ' \
           f'"P_SE": {json_line["SE"]}, ' \
           f'"P_P": {json_line["P_VALUE"]}, ' \
           f'"NEG_LOG_P": {json_line["NEG_LOG_P"]}, ' \
           f'"ABS_POS": {abs_pos}, ' \
           f'"TRAIT": "{phenotype}", ' \
           f'"PRIOR": {json_line["PRIOR"]}, ' \
           f'"CHR": "{json_line["CHR"]}", ' \
           f'"START": {json_line["START"]}, ' \
           f'"END": {json_line["END"]}, ' \
           f'"NEAREST_TO_LEAD": {make_bool(json_line["NEAREST_TO_LEAD"])}, ' \
           f'"CLUMP": {make_str_option(json_line["CLUMP"])}, ' \
           f'"NORM_PROBABILITY": {json_line["NORM_PROBABILITY"]}}}\n'


def translate_variants(json_line, phenotype):
    return f'{{"RSID": "{json_line["RSID"]}", ' \
           f'"CHR": {json_line["CHR"]}, ' \
           f'"POS": {json_line["POS"]}, ' \
           f'"P_BETA": {json_line["BETA"]}, ' \
           f'"PIP": {json_line["PROBABILITY"]}, ' \
           f'"GENE_1": {make_str_option(json_line["GENE_1"])}, ' \
           f'"LINK_SC_1": {json_line["LINK_SC_1"]}, ' \
           f'"GENE_2": {make_str_option(json_line["GENE_2"])}, ' \
           f'"LINK_SC_2": {json_line["LINK_SC_2"]}, ' \
           f'"GENE_3": {make_str_option(json_line["GENE_3"])}, ' \
           f'"LINK_SC_3": {json_line["LINK_SC_3"]}, ' \
           f'"TRAIT": "{phenotype}", ' \
           f'"REF": "{json_line["REF"]}", ' \
           f'"ALT": "{json_line["ALT"]}", ' \
           f'"PRIOR": {json_line["PRIOR"]}, ' \
           f'"SE": {json_line["SE"]}, ' \
           f'"Z_SCORE": {json_line["Z_SCORE"]}, ' \
           f'"P_VALUE": {json_line["P_VALUE"]}, ' \
           f'"S2G_GENES": {make_str_option(json_line["S2G_genes"])}, ' \
           f'"S2G_SCORES": {make_str_option(json_line["S2G_scores"])}, ' \
           f'"NORM_BETA": {json_line["NORM_BETA"]}, ' \
           f'"LOCAL_SIGMA_2": {json_line["LOCAL_SIGMA_2"]}, ' \
           f'"LEAD_SNP": {make_bool(json_line["LEAD_SNP"])}, ' \
           f'"NEAREST_GENE": {make_str_option(json_line["NEAREST_GENE"])},' \
           f'"NEAREST_DISTANCE": {json_line["NEAREST_DISTANCE"]}, ' \
           f'"CLUMP": "{json_line["CLUMP"]}", ' \
           f'"GWAS_Z": {json_line["GWAS_Z"]}, ' \
           f'"GWAS_P": {json_line["GWAS_P"]}, ' \
           f'"GWAS_BETA": {json_line["GWAS_BETA"]}, ' \
           f'"GWAS_SE": {json_line["GWAS_SE"]}, ' \
           f'"GWAS_AF": {json_line["GWAS_AF"]}, ' \
           f'"GWAS_N": {json_line["GWAS_N"]}}}\n'


def translate_v2g(json_line, phenotype):
    return f'{{"rsID": "{json_line["rsID"]}", ' \
           f'"Gene": "{json_line["Gene"]}", ' \
           f'"Value": {json_line["Value"]}, ' \
           f'"PHENOTYPE": "{phenotype}"}}\n'


def translate(phenotype, data_type, line_fnc):
    with open(f'outputs/{data_type}.json', 'w') as f_out:
        for chromosome in CHROMOSOMES:
            if os.path.exists(f'inputs/falcon.{chromosome}.{data_type}'):
                with open(f'inputs/falcon.{chromosome}.{data_type}', 'r') as f_in:
                    header = f_in.readline().strip().split('\t')
                    for line in f_in:
                        json_line = dict(zip(header, line.strip().split('\t')))
                        f_out.write(line_fnc(json_line, phenotype))
    upload_data(phenotype, data_type)


def success(file_path):
    subprocess.check_call(['touch', '_SUCCESS'])
    subprocess.check_call(['aws', 's3', 'cp', '_SUCCESS', file_path])
    os.remove('_SUCCESS')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--phenotype', default=None, required=True, type=str,
                        help="Input phenotype.")
    args = parser.parse_args()

    os.makedirs('inputs', exist_ok=True)
    os.makedirs('outputs', exist_ok=True)
    download_data(args.phenotype)
    translate(args.phenotype, 'genes', translate_genes)
    translate(args.phenotype, 'variants', translate_variants)
    translate(args.phenotype, 'v2g', translate_v2g)
    shutil.rmtree('inputs')
    shutil.rmtree('outputs')


if __name__ == '__main__':
    main()
