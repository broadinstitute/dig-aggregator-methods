#!/usr/bin/python3
import argparse
import gzip
import os
import shutil
import subprocess

s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']


# Cannot include a biosample with 0 SNPs
def check_biosample(annotation, tissue, biosample):
    total_m_50 = 0
    for CHR in range(1, 23):
        with open(f'{biosample_to_file(annotation, tissue, biosample)}.{CHR}.l2.M_5_50', 'r') as f:
            total_m_50 += int(f.readline().strip())
    return total_m_50 > 0


def download_files(ancestry, annotation, tissue):
    subprocess.check_call(['aws', 's3', 'cp',
                           f'{s3_in}/out/ldsc/regions/ld_score/ancestry={ancestry}/annotation/{annotation}/ld_score.zip',
                           f'ld_files/annotation/{annotation}/'])
    subprocess.check_call(f'unzip ld_files/annotation/{annotation}/ld_score.zip -d ld_files/annotation/{annotation}/', shell=True)
    os.remove(f'ld_files/annotation/{annotation}/ld_score.zip')
    subprocess.check_call(['aws', 's3', 'cp',
                           f'{s3_in}/out/ldsc/regions/ld_score/ancestry={ancestry}/annotation-tissue/{annotation}___{tissue}/ld_score.zip',
                           f'ld_files/annotation-tissue/{annotation}___{tissue}/'])
    subprocess.check_call(f'unzip ld_files/annotation-tissue/{annotation}___{tissue}/ld_score.zip -d ld_files/annotation-tissue/{annotation}___{tissue}/', shell=True)
    os.remove(f'ld_files/annotation-tissue/{annotation}___{tissue}/ld_score.zip')

    biosamples = []
    s3_ls = subprocess.check_output(['aws', 's3', 'ls', f'{s3_in}/out/ldsc/regions/ld_score/ancestry={ancestry}/annotation-tissue-biosample/']).decode()
    for line in s3_ls.strip().split('\n'):
        s3_folder = line.split(' ')[-1].split('/')[0]
        s3_annotation, s3_tissue, s3_biosample = s3_folder.split('___')
        if s3_annotation == annotation and s3_tissue == tissue:
            subprocess.check_call(['aws', 's3', 'cp',
                                   f'{s3_in}/out/ldsc/regions/ld_score/ancestry={ancestry}/annotation-tissue-biosample/{s3_folder}/ld_score.zip',
                                   f'ld_files/annotation-tissue-biosample/{s3_folder}/'])
            subprocess.check_call(f'unzip ld_files/annotation-tissue-biosample/{s3_folder}/ld_score.zip -d ld_files/annotation-tissue-biosample/{s3_folder}/', shell=True)
            os.remove(f'ld_files/annotation-tissue-biosample/{s3_folder}/ld_score.zip')
            if check_biosample(annotation, tissue, s3_biosample):
                biosamples.append(s3_biosample)
    return biosamples


def biosample_to_file(annotation, tissue, biosample):
    return f'ld_files/annotation-tissue-biosample/{annotation}___{tissue}___{biosample}/ld'


def tissue_to_file(annotation, tissue):
    return f'ld_files/annotation-tissue/{annotation}___{tissue}/ld'


def annotation_file(annotation):
    return f'ld_files/annotation/{annotation}/ld'


def get_biosample_files(annotation, tissue, biosamples):
    return [annotation_file(annotation)] + \
           [biosample_to_file(annotation, tissue, biosample) for biosample in biosamples]


def get_tissue_files(annotation, tissue):
    return [annotation_file(annotation), tissue_to_file(annotation, tissue)]


def combine_annot(files, header, sub_region, annotation, tissue, CHR):
    extension = 'annot.gz'
    print(extension, CHR)
    open_files = [gzip.open(f'{f}.{CHR}.{extension}', 'r') for f in files]
    with gzip.open(f'ld_files/combined/{sub_region}/{annotation}.{tissue}.{CHR}.' + extension, 'w') as f_out:
        f_out.write(b'\t'.join(header) + b'\n')
        _ = [f.readline().strip() for f in open_files]  # unused header
        lines = [f.readline().strip() for f in open_files]
        while all([len(line) > 0 for line in lines]):
            f_out.write(b'\t'.join(lines) + b'\n')
            lines = [f.readline().strip() for f in open_files]


def combine_ldscore(files, header, sub_region, annotation, tissue, CHR):
    extension = 'l2.ldscore.gz'
    print(extension, CHR)
    open_files = [gzip.open(f'{f}.{CHR}.{extension}', 'r') for f in files]
    with gzip.open(f'ld_files/combined/{sub_region}/{annotation}.{tissue}.{CHR}.' + extension, 'w') as f_out:
        f_out.write(b'\t'.join([b'CHR', b'SNP', b'BP'] + header) + b'\n')
        _ = [f.readline().strip() for f in open_files]  # unused header
        lines = open_files[0].readline().strip().split(b'\t') + \
                [f.readline().strip().split(b'\t')[-1] for f in open_files[1:]]
        while all([len(line) > 0 for line in lines]):
            f_out.write(b'\t'.join(lines) + b'\n')
            lines = open_files[0].readline().strip().split(b'\t') + \
                    [f.readline().strip().split(b'\t')[-1] for f in open_files[1:]]


def combine_non_gzip(files, sub_region, annotation, tissue, CHR, extension):
    print(extension, CHR)
    open_files = [open(f'{f}.{CHR}.{extension}', 'r') for f in files]
    with open(f'ld_files/combined/{sub_region}/{annotation}.{tissue}.{CHR}.' + extension, 'w') as f_out:
        f_out.write('\t'.join([f.readline().strip() for f in open_files]) + '\n')


def combine_sub_region(files, header, sub_region, annotation, tissue):
    for CHR in range(1, 23):
        combine_annot(files, header, sub_region, annotation, tissue, CHR)
        combine_ldscore(files, header, sub_region, annotation, tissue, CHR)
        combine_non_gzip(files, sub_region, annotation, tissue, CHR, 'l2.M')
        combine_non_gzip(files, sub_region, annotation, tissue, CHR, 'l2.M_5_50')


def combine_biosamples(annotation, tissue, biosamples):
    header = [annotation.encode()] + [biosample.encode() for biosample in biosamples]
    files = get_biosample_files(annotation, tissue, biosamples)
    sub_region = 'annotation-tissue-biosample'
    combine_sub_region(files, header, sub_region, annotation, tissue)


def combine_tissue(annotation, tissue):
    header = [annotation.encode(), tissue.encode()]
    files = get_tissue_files(annotation, tissue)
    sub_region = 'annotation-tissue'
    combine_sub_region(files, header, sub_region, annotation, tissue)


def upload_and_remove(ancestry, annotation, tissue):
    subprocess.check_call(['touch', 'ld_files/combined/annotation-tissue/_SUCCESS'])
    subprocess.check_call(['aws', 's3', 'cp',
                           'ld_files/combined/annotation-tissue/',
                           f'{s3_out}/out/ldsc/regions/combined_ld/ancestry={ancestry}/annotation-tissue/{annotation}___{tissue}/', '--recursive'])
    subprocess.check_call(['touch', 'ld_files/combined/annotation-tissue-biosample/_SUCCESS'])
    subprocess.check_call(['aws', 's3', 'cp',
                           'ld_files/combined/annotation-tissue-biosample/',
                           f'{s3_out}/out/ldsc/regions/combined_ld/ancestry={ancestry}/annotation-tissue-biosample/{annotation}___{tissue}/', '--recursive'])
    shutil.rmtree('ld_files')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--ancestry', required=True, type=str,
                        help="Ancestry (g1000 style e.g. EUR)")
    parser.add_argument('--annotation', required=True, type=str,
                        help="Sub region name (e.g. accessible-chromatin)")
    parser.add_argument('--tissue', required=True, type=str,
                        help="A tissue to combine (e.g. pancreas)")
    args = parser.parse_args()
    ancestry = args.ancestry
    annotation = args.annotation
    tissue = args.tissue
    biosamples = download_files(ancestry, annotation, tissue)
    os.mkdir('ld_files/combined')
    os.mkdir('ld_files/combined/annotation-tissue')
    os.mkdir('ld_files/combined/annotation-tissue-biosample')
    combine_biosamples(annotation, tissue, biosamples)
    combine_tissue(annotation, tissue)
    upload_and_remove(ancestry, annotation, tissue)


if __name__ == '__main__':
    main()
