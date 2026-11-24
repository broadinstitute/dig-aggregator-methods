#!/usr/bin/python3
import argparse
import datetime
import json
import os
import subprocess

downloaded_files = '/mnt/var/pigean'
s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']

def get_model_data():
    with open(f'{downloaded_files}/aws_pigean_models_s3.json', 'r') as f:
        models = json.load(f)
    return ({model['name']: model for model in models['models']},
            {gene_set['name']: gene_set for gene_set in models['gene_sets']})


def file_names(trait_type):
    if trait_type == 'sumstats':
        return ['pigean.sumstats.gz']
    elif trait_type == 'gene_lists':
        return ['gene_list.tsv']
    elif trait_type == 'exomes':
        return ['exomes.sumstats.gz']
    elif trait_type == 'exomes___sumstats':
        return ['exomes.sumstats.gz', 'pigean.sumstats.gz']
    else:
        raise ValueError(f'Invalid trait_type: {trait_type}')


def download_data(trait_type, trait_group, phenotype):
    for file_name in file_names(trait_type):
        file_path = f'{s3_in}/out/pigean/inputs/{trait_type}/{trait_group}/{phenotype}/{file_name}'
        subprocess.check_call(['aws', 's3', 'cp', file_path, '.'])


def get_gene_sets(model):
    models, gene_sets = get_model_data()
    model_info = models[model]
    inputs = []
    p_infs = []
    for gene_set in model_info['gene_sets']:
        gene_set_info = gene_sets[gene_set]
        if gene_set_info['type'] == 'set':
            inputs += ['--X-in', f'{downloaded_files}/{gene_set_info["file"]}']
        else:
            inputs += ['--X-list', f'{downloaded_files}/{gene_set_info["name"]}/{gene_set_info["file"]}']
        p_infs += ['--p-noninf', str(gene_set_info['p-inf'])]
    if len(inputs) > 0:
        return inputs + p_infs
    else:
        raise Exception(f'Invalid gene set size {model}')


def trait_type_command(trait_type):
    cmds = []
    files_names = file_names(trait_type)
    for file_name in files_names:
        if file_name == 'pigean.sumstats.gz':
            cmds += ['--gwas-in', os.path.abspath(file_name),
                     '--gwas-chrom-col', 'CHROM',
                     '--gwas-pos-col', 'POS',
                     '--gwas-p-col', 'P',
                     '--gwas-n-col', 'N',
                     '--gwas-z-source', 'p'
                     ]
        elif file_name == 'gene_list.tsv':
            cmds += [
                '--gene-list-in', os.path.abspath(file_name),
                '--gene-list-id-col', '1',
                '--gene-list-prob-col', '2',
                '--gene-list-no-header',
                '--gene-universe-in', f'{downloaded_files}/validation_main.refGene_hg19_TSS.subset.loc',
                '--gene-universe-no-header',
                '--gene-universe-id-col', '1',
                '--background-prior', '0.05'
            ]
        elif file_name == 'exomes.sumstats.gz':
            cmds += [
                '--exomes-in', os.path.abspath(file_name),
                '--exomes-gene-col', 'Gene',
                '--exomes-p-col', 'P-value',
                '--exomes-beta-col', 'Effect'
            ]
    return cmds


base_cmd = [
    'python3.11', '-m', 'pigean', 'gibbs',
    '--gene-loc-file', f'{downloaded_files}/validation_main.NCBI37.3.plink.gene.loc',
    '--gene-map-in', f'{downloaded_files}/validation_main.portal_gencode.gene.map',
    '--gene-loc-file-huge', f'{downloaded_files}/validation_main.refGene_hg19_TSS.subset.loc',
    '--exons-loc-file-huge', f'{downloaded_files}/validation_main.NCBI37.3.plink.gene.exons.loc',
    '--seed', '1', '--deterministic',
    '--sigma-power', '-2',
    '--update-hyper', 'none',
    '--filter-gene-set-p', '0.005',
    '--max-num-gene-sets', '5000',
    '--output-detail', 'full',
    '--write-gibbs-global-filtered-summaries',
    '--num-chains', '10',
    '--disable-stall-detection',
    '--gibbs-schedule', 'retained',
    '--max-gb', '32.0',
    '--debug-level', '3',
    '--track-filtered-beta-uncorrected-mode', 'none',
    '--gibbs-engine', 'parallel-prefilter',
    '--gibbs-enable', 'inner-matmul,inner-fused,inner-batching,compiled-prefilter,inner-cache64,huge-prior-softmax,static-correlation,compiled-moments',
    '--gene-stats-out', os.path.abspath('outputs/gs.out'),
    '--gene-set-stats-out', os.path.abspath('outputs/gss.out'),
    '--gene-gene-set-stats-out', os.path.abspath('outputs/ggss.out'),
    '--gene-effectors-out', os.path.abspath('outputs/ge.out'),
    '--params-out', os.path.abspath('outputs/p.out')
]


def run_pigean(trait_type, model):
    os.makedirs('outputs', exist_ok=True)
    cmd = base_cmd + trait_type_command(trait_type) + get_gene_sets(model)
    subprocess.check_call(cmd, cwd=f'{downloaded_files}/pigean/src')


def make_metadata(trait_type, trait_group, phenotype, model, started, finished, success):
    with open('run_metadata.txt', 'w') as f:
        f.write('repository=https://github.com/flannick/pigean.git\n')
        f.write('commit=8f4c619c2c946b62fc303dc60af82989820bc727\n')
        f.write(f'trait_group={trait_group}\n')
        f.write(f'phenotype={phenotype}\n')
        f.write(f'model={model}\n')
        for file_name in file_names(trait_type):
            f.write(f'input={s3_in}/out/pigean/inputs/{trait_type}/{trait_group}/{phenotype}/{file_name}\n')
        f.write(f'eaggl_output={s3_out}/out/pigean/staging/pigean/{trait_group}/{phenotype}/{model}\n')
        f.write(f'started_utc={started}\n')
        f.write(f'completed_utc={finished}\n')
        f.write(f'status={success}\n')


def success(file_path):
    subprocess.check_call(['touch', '_SUCCESS'])
    subprocess.check_call(['aws', 's3', 'cp', '_SUCCESS', file_path])
    os.remove('_SUCCESS')


def upload(file_name, file_path):
    if os.path.exists(file_name):
        subprocess.check_call(['aws', 's3', 'cp', file_name, file_path])
        os.remove(file_name)


def upload_data(trait_group, phenotype, model):
    file_path = f'{s3_out}/out/pigean/staging/pigean/{trait_group}/{phenotype}/{model}/'
    for file in ['gs.out', 'gss.out', 'ggss.out', 'ge.out', 'p.out']:
        upload(f'outputs/{file}', file_path)
    success(file_path)


def upload_metadata(trait_group, phenotype, model):
    file_path = f'{s3_out}/out/pigean/staging/pigean/{trait_group}/{phenotype}/{model}/'
    upload('run_metadata.txt', file_path)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--trait-type', default=None, required=True, type=str,
                        help="sumstats or gene_lists")
    parser.add_argument('--trait-group', default=None, required=True, type=str,
                        help="Trait group")
    parser.add_argument('--phenotype', default=None, required=True, type=str,
                        help="Input phenotype.")
    parser.add_argument('--model', default=None, required=True, type=str,
                        help="model (mouse_msigdb).")
    args = parser.parse_args()
    download_data(args.trait_type, args.trait_group, args.phenotype)
    started = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
    try:
        run_pigean(args.trait_type, args.model)
        upload_data(args.trait_group, args.phenotype, args.model)
        for file_name in file_names(args.trait_type):
            os.remove(file_name)
        success = 'success'
    except:
        print('ERROR')
        success = 'failed'
    finished = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
    make_metadata(args.trait_type, args.trait_group, args.phenotype, args.model, started, finished, success)
    upload_metadata(args.trait_group, args.phenotype, args.model)


if __name__ == '__main__':
    main()


