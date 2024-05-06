#!/usr/bin/python3
from optparse import OptionParser
import pandas as pd
import numpy as np
import shutil
import subprocess
import os

s3_in=os.environ['INPUT_PATH']
s3_out=os.environ['OUTPUT_PATH']

# def finds json files in the directory
def make_json_files(directory):
	subprocess.check_call(['aws', 's3', 'cp', directory, 'input/', '--recursive'])
	subprocess.run('cat input/*.json > input.json', shell=True)
	shutil.rmtree('input')

def make_ld_files(directory):
	subprocess.check_call(['aws', 's3', 'cp', directory, 'input/', '--recursive'])
	subprocess.run('cat input/*.ld > snp_ld.ld', shell=True)
	shutil.rmtree('input')

def main():
	usage = "usage: %prog [options]"
	parser = OptionParser(usage)
	parser.add_option("","--phenotype", default=None)
	parser.add_option("","--ancestry", default=None)

	(options, args) = parser.parse_args()

	clump_path = f'{s3_in}/out/metaanalysis/bottom-line/ancestry-clumped/{options.phenotype}/ancestry={options.ancestry}'
	var2rs_path = '/mnt/var/susie/snps.csv'
	out_path = f'{s3_out}/out/susie/staging/{options.phenotype}/ancestry={options.ancestry}' 
	
	# read all files in the clump path
	make_json_files(clump_path)

	# read var2rs file
	df_var_rs_Id = pd.read_csv(var2rs_path,sep='\t')

	# create the tmp out directory
	out_directory = 'data'
	if not os.path.exists(out_directory):
		os.makedirs(out_directory,exist_ok=True)

	# read clump
	df_clump = pd.read_json('input.json', lines=True)

	# sort clump based on the varId
	df_clump.sort_values('varId',inplace = True)
	df_var_filter = df_var_rs_Id[df_var_rs_Id['varId'].isin(df_clump['varId'])]

	# only common variants 
	df_clump = df_clump[df_clump['varId'].isin(df_var_filter['varId'])]
	df_clump.sort_values('varId',inplace = True)
	df_var_filter.sort_values('varId',inplace = True)


	# add dbSNP into the clump files
	df_clump['dbSNP'] = df_var_filter['dbSNP'].to_numpy()

	df_clump = df_clump.rename(columns={'dbSNP':'rsId','reference':'ref'})

	# for loop over clump ids
	for i in sorted(df_clump['clump'].unique()):
		# filter gwas based on the clump id
		df_susie = df_clump[df_clump['clump']==i]
		chrom = df_susie['chromosome'].to_numpy()[0]
		gwas_susie_file_name = out_directory+'/'+'clump_'+str(i)+'.csv'
		df_susie.to_csv(gwas_susie_file_name,sep='\t',index=False)
		df_susie['rsId'].to_csv(f'{out_directory}/snps.txt',sep='\t',index=False,header=False)

		# calculate LD for snps list
		subprocess.call(["bash", "/mnt/var/susie/plink_ld_snp_list.sh", f'{chrom}', f'{out_directory}/snps.txt', f'{out_directory}/snps_ld'])

		# Call the Bash script of SuSiE with its arguments 
		argument1_gwas = gwas_susie_file_name
		argument2_ld   = f'{out_directory}/snps_ld.ld'
		argument3_out  = out_directory
		subprocess.call(['Rscript','/mnt/var/susie/SuSiE.r','--gwas',gwas_susie_file_name,'--ld',argument2_ld, '--out',argument3_out])
		os.remove(argument1_gwas)

	os.remove(f'{out_directory}/snps.txt')
	os.remove(f'{out_directory}/snps_ld.ld')
	os.remove(f'{out_directory}/snps_ld.log')
	os.remove(f'{out_directory}/snps_ld.nosex')
	subprocess.check_call(['touch', f'{out_directory}/_SUCCESS'])	
	subprocess.check_call(['aws','s3','cp',f'{out_directory}/',out_path,'--recursive'])
	os.remove('input.json')
	shutil.rmtree(out_directory)

if __name__ == '__main__':
	main()
