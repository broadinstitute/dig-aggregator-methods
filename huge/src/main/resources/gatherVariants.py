import bisect
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit


# EC2 development localhost directories
# filepath = '/Users/kmoogala/Downloads/part-00000-cc551958-d129-4d9a-82f1-d7a039db68cb-c000.json'
SRCDIR = 's3a://dig-analysis-data/out/metaanalysis/trans-ethnic/'
phenotype = "2hrG"
threshold_p = 0.000000005

def main():
    """
    Arguments: none
    """
    spark = SparkSession.builder.appName('magma').getOrCreate()
    # load all json files for given phenotype
    src_dir = SRCDIR + phenotype
    variants = spark.read.json(f'{src_dir}/part-*')

    # filter out all variants with p-values greater than the threshold value
    variants.show(40)
    variants = variants.filter(variants.pValue <= threshold_p)
    variants.show(40)

    # variants_file = spark.read.json(filepath)
    '''
    # seprate variants by chromosome
    chromosome_list = {i: [] for i in range(1, 24)}
    for variant in variants:
        print(variant)
        if variant['chromosome'] in chromosome_list:
            chromosome_list[variant['chromosome']].append(variant)


    # sort variants by position
    for chromosome_variants in chromosome_list.values():
        chromosome_variants.sort(key=lambda x: int(x["position"]))
    '''
    # open genes json file
    gene_file = '/Users/kmoogala/Downloads/part-00000-98e53cb2-5674-11eb-9e7f-305a3a47f41a.json'

    # load into json object
    gene_locations = spark.read.json(gene_file)

    #add null columns/remove columns needed to merge
    '''
    gene_locations = gene_locations.withColumn("position", lit(None))
    variants = variants.drop('zScore')
    variants = variants.drop('beta')
    '''
    # join two dataframes by matching chromosome and position
    buffer = 50
    cond = (gene_locations.chromosome == variants.chromosome) & \
           (gene_locations.start - buffer <= variants.position) & \
           (gene_locations.end + buffer >= variants.position)
    gene_locations = gene_locations.join(variants.alias('variants'), cond, "left_outer")
    gene_locations = gene_locations.sort('variants.chromosome', 'position')
    gene_locations = gene_locations.sort(gene_locations.name)

    # variants = variants.join(gene_locations.alias('gene_locations'), cond, "inner")
    # variants = variants.sort('gene_locations.chromosome', 'position')
    # variants[variants.position.isNotNull()].show()
    gene_locations[gene_locations.position.isNotNull()].show(200)
    # gene_locations.show(150)

    '''
    # iterate through genes and find in variants
    for gene in gene_locations:
        chromosome = gene['chromosome']
        start = gene['start'] - buffer
        end = gene['end'] + buffer

        start_pos = bisect.bisect_left(chromosome_list[chromosome], start, key=lambda x: int(x["position"]))
        end_pos = bisect.bisect_right(chromosome_list[chromosome], end, key=lambda x: int(x["position"]))
        # use binary search to find the start
        # total_variants = findVariants(chromosome_list[chromosome], start, end)
        sig_variants = chromosome_list[chromosome][start_pos:end_pos]
        print(sig_variants)
    '''

'''
def findVariants(chromosome_data, start, end):

    low = 0
    high = len(chromosome_data)
    mid = 0

    # find start position within chromosome
    while low <= high:
        mid = (high+low)/2

        if chromosome_data[mid]['position'] < start:
            low = mid + 1
        elif chromosome_data[mid]['position'] > start:
            high = mid - 1
        else:
            break

    startIndex = mid

    # find end position within chromosome
    while low <= high:
        mid = (high+low)/2
        if chromosome_data[mid]['position'] < end:
            low = mid + 1
        elif chromosome_data[mid]['position'] > end:
            high = mid - 1
        else:
            break

    endIndex = mid
    return chromosome_data[startIndex:endIndex]
    
    

def p_val_filter(value):
    if value >= threshold_p:
        return False
    else:
        return True


'''
"""
def chromosomeSearch(variantsList, chromosome):
    low = 0
    high = len(variantsList)
    mid = 0
    begin = 0
    end = 0

    while low <= high:
        mid = (high+low) //2

        #if chromosome is greater, search right half
        if variantsList[mid]['chromosome'] < chromosome:
            low = mid + 1

        #if chromosome is less than, search left half
        elif variantsList[mid]['chromosome'] > chromosome:
            high = mid - 1

        #if neither, chromosome is found
        else:
            break

    startMid = mid
    endMid = mid
    while chromosome == variantsList[startMid]['chromosome']:
        startMid = startMid - 1

    return startMid

def variantSearch(variants, startChromosome, startPosition, endPosition):
    i = startChromosome
    variantCounter = 0
    while variants[i]['position'] < endPosition:
        if variants[i]['position'] >= startPosition:
            variantCounter = variantCounter + 1

    return variantCounter



class Gene:
    def __init__(self, name, chromosome, startLoc):
        self.name = name
        self.chromosome = chromosome
        self.startLoc = startLoc
"""

if __name__ == '__main__':
    main()
