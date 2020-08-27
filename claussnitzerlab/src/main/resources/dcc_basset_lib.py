# imports
import twobitreader
from twobitreader import TwoBitFile
import numpy as np 
import torch
from torch import nn
from sklearn.preprocessing import OneHotEncoder
import csv
import re
import json

print("have pytorch version {}".format(torch.__version__))
print("have numpy version {}".format(np.__version__))

# add in model classes
class LambdaBase(nn.Sequential):
    def __init__(self, fn, *args):
        super(LambdaBase, self).__init__(*args)
        self.lambda_func = fn

    def forward_prepare(self, input):
        output = []
        for module in self._modules.values():
            output.append(module(input))
        return output if output else input

class Lambda(LambdaBase):
    def forward(self, input):
        return self.lambda_func(self.forward_prepare(input))


# method to create string sequence from position
def get_genomic_sequence(position, offset, chromosome, alt_allele=None):
    if alt_allele is not None:
        sequence = chromosome[position - offset : position - 1] + alt_allele + chromosome[position : position + offset]
    else:
        sequence = chromosome[position - offset: position + offset]

    # trim to size
    if len(sequence) > offset * 2:
        sequence = sequence[:offset * 2]

    # return
    return sequence.upper()

def get_ref_alt_sequences(position, offset, chromosome, alt_allele=None):
    ref_sequence = get_genomic_sequence(position, offset, chromosome)
    alt_sequence = get_genomic_sequence(position, offset, chromosome, alt_allele)

    return ref_sequence, alt_sequence

def get_input_np_array(sequence_list):
    sequence_np = None
    for seq in sequence_list:
        if sequence_np is None:
            sequence_np = np.array(list(seq))
        else:
            sequence_np = np.vstack((sequence_np, np.array(list(seq))))

    # return
    return sequence_np

def get_one_hot_sequence_array(sequence_list):
    # get the numpy sequence
    sequence_np = get_input_np_array(sequence_list)

    # use the numpy utility to replace the letters by numbers
    sequence_np[sequence_np == 'A'] = 0
    sequence_np[sequence_np == 'C'] = 1
    sequence_np[sequence_np == 'G'] = 2
    sequence_np[sequence_np == 'T'] = 3

    # convert to ints
    try:
        sequence_np = sequence_np.astype(np.int)
    except:
        raise ValueError("got error for sequence \n{}".format(sequence_np))

    # one hot the sequence
    number_classes = 4
    sequence_np = np.eye(number_classes)[sequence_np]

    # return
    return sequence_np

def get_variant_list(file):
    variants = []
    with open(file, 'r') as variant_file:

        # read all the next rows
        for line in variant_file:
            row = json.loads(line)
            # print(row)
            variants.append(row['varId'])


    # print the first 10 variants
    # for index in range(1, 10):
    #     print("got variant: {}".format(variants[index]))

    # return
    return variants

def load_beluga_model(weights_file, should_log=True):
    # load the weights
    state_dict = torch.load(weights_file)
    pretrained_model_reloaded_th = load_beluga_model_from_state_dict(state_dict, should_log)

    # return
    return pretrained_model_reloaded_th

def load_basset_model(weights_file, should_log=True):
    # load the weights
    state_dict = torch.load(weights_file)
    pretrained_model_reloaded_th = load_basset_model_from_state_dict(state_dict, should_log)

    # return
    return pretrained_model_reloaded_th

def load_nasa_model(weights_file, should_log=True):
    # load the weights
    state_dict = torch.load(weights_file)
    pretrained_model_reloaded_th = load_nasa_model_from_state_dict(state_dict, should_log)

    # return
    return pretrained_model_reloaded_th

def load_beluga_model_from_state_dict(state_dict, should_log=True):
    # load the DeepSEA Beluga model
    pretrained_model = nn.Sequential(
            nn.Sequential(
                nn.Conv2d(4, 320, (1, 8)),
                nn.ReLU(),
                nn.Conv2d(320, 320, (1, 8)),
                nn.ReLU(),
                nn.Dropout(0.2),
                nn.MaxPool2d((1, 4), (1, 4)),
                nn.Conv2d(320, 480, (1, 8)),
                nn.ReLU(),
                nn.Conv2d(480, 480, (1, 8)),
                nn.ReLU(),
                nn.Dropout(0.2),
                nn.MaxPool2d((1, 4), (1, 4)),
                nn.Conv2d(480, 640, (1, 8)),
                nn.ReLU(),
                nn.Conv2d(640, 640, (1, 8)),
                nn.ReLU(),
            ),
            nn.Sequential(
                nn.Dropout(0.5),
                Lambda(lambda x: x.view(x.size(0), -1)),
                nn.Sequential(Lambda(lambda x: x.view(1, -1) if 1 == len(x.size()) else x), nn.Linear(67840, 2003)),
                nn.ReLU(),
                nn.Sequential(Lambda(lambda x: x.view(1, -1) if 1 == len(x.size()) else x), nn.Linear(2003, 2002)),
            ),
            nn.Sigmoid(),
        )

    # print
    if should_log:
        print("got DeepSEA Beluga model of type {}".format(type(pretrained_model)))

    # load the weights
    pretrained_model.load_state_dict(state_dict)

    # return
    return pretrained_model

def load_basset_model_from_state_dict(state_dict, should_log=True):
    # load the Basset model
    pretrained_model_reloaded_th = nn.Sequential( # Sequential,
            nn.Conv2d(4,300,(19, 1)),
            nn.BatchNorm2d(300),
            nn.ReLU(),
            nn.MaxPool2d((3, 1),(3, 1)),
            nn.Conv2d(300,200,(11, 1)),
            nn.BatchNorm2d(200),
            nn.ReLU(),
            nn.MaxPool2d((4, 1),(4, 1)),
            nn.Conv2d(200,200,(7, 1)),
            nn.BatchNorm2d(200),
            nn.ReLU(),
            nn.MaxPool2d((4, 1),(4, 1)),
            Lambda(lambda x: x.view(x.size(0),-1)), # Reshape,
            nn.Sequential(Lambda(lambda x: x.view(1,-1) if 1==len(x.size()) else x ),nn.Linear(2000,1000)), # Linear,
            nn.BatchNorm1d(1000,1e-05,0.1,True),#BatchNorm1d,
            nn.ReLU(),
            nn.Dropout(0.3),
            nn.Sequential(Lambda(lambda x: x.view(1,-1) if 1==len(x.size()) else x ),nn.Linear(1000,1000)), # Linear,
            nn.BatchNorm1d(1000,1e-05,0.1,True),#BatchNorm1d,
            nn.ReLU(),
            nn.Dropout(0.3),
            nn.Sequential(Lambda(lambda x: x.view(1,-1) if 1==len(x.size()) else x ),nn.Linear(1000,164)), # Linear,
            nn.Sigmoid(),
        )

    # print
    if should_log:
        print("got Basset model of type {}".format(type(pretrained_model_reloaded_th)))

    # load the weights
    pretrained_model_reloaded_th.load_state_dict(state_dict)

    # return
    return pretrained_model_reloaded_th

def load_nasa_model_from_state_dict(state_dict, should_log=True):
    # load the Basset model
    pretrained_model_reloaded_th = nn.Sequential( # Sequential,
        nn.Conv2d(4,400,(21, 1),(1, 1),(10, 0)),
        nn.BatchNorm2d(400),
        nn.ReLU(),      # NO WEIGHTS
        nn.MaxPool2d((3, 1),(3, 1),(0, 0),ceil_mode=True),      # NO WEIGHTS
        nn.Conv2d(400,300,(11, 1),(1, 1),(5, 0)),
        nn.BatchNorm2d(300),
        nn.ReLU(),      # NO WEIGHTS
        nn.MaxPool2d((4, 1),(4, 1),(0, 0),ceil_mode=True),      # NO WEIGHTS
        nn.Conv2d(300,300,(7, 1),(1, 1),(3, 0)),
        nn.BatchNorm2d(300),
        nn.ReLU(),      # NO WEIGHTS
        nn.MaxPool2d((4, 1),(4, 1),(0, 0),ceil_mode=True),      # NO WEIGHTS
        nn.Conv2d(300,300,(5, 1),(1, 1),(2, 0)),
        nn.BatchNorm2d(300),
        nn.ReLU(),      # NO WEIGHTS
        nn.MaxPool2d((4, 1),(4, 1),(0, 0),ceil_mode=True),      # NO WEIGHTS
        Lambda(lambda x: x.view(x.size(0),-1)), # Reshape,      # NO WEIGHTS
        nn.Sequential(      
            Lambda(lambda x: x.view(1,-1) if 1==len(x.size()) else x ),     # NO WEIGHTS
            nn.Linear(1500,1024)), # Linear,
        nn.BatchNorm1d(1024,1e-05,0.1,True), #BatchNorm1d,
        nn.ReLU(),              # NO WEIGHTS
        nn.Dropout(0.3),        # NO WEIGHTS
        nn.Sequential(
            Lambda(lambda x: x.view(1,-1) if 1==len(x.size()) else x ),     # NO WEIGHTS
            nn.Linear(1024,512)), # Linear,
        nn.BatchNorm1d(512,1e-05,0.1,True),#BatchNorm1d,
        nn.ReLU(),          # NO WEIGHTS
        nn.Dropout(0.3),    # NO WEIGHTS
        nn.Sequential(
            Lambda(lambda x: x.view(1,-1) if 1==len(x.size()) else x ),  # NO WEIGHTS
            nn.Linear(512,167)), # Linear,
        nn.Sigmoid(),       # NO WEIGHTS
    )
        # pretrained_model_reloaded_th = nn.Sequential( # Sequential,
        #     nn.Conv2d(4,400,(21, 1)),
        #     nn.BatchNorm2d(400),
        #     nn.ReLU(),
        #     nn.MaxPool2d((3, 1),(3, 1)),
        #     nn.Conv2d(400,300,(11, 1)),
        #     nn.BatchNorm2d(300),
        #     nn.ReLU(),
        #     nn.MaxPool2d((4, 1),(4, 1)),
        #     nn.Conv2d(300,300,(7, 1)),
        #     nn.BatchNorm2d(300),
        #     nn.ReLU(),
        #     nn.MaxPool2d((4, 1),(4, 1)),
        #     nn.Conv2d(300,300,(5, 1)),
        #     nn.BatchNorm2d(200),
        #     nn.ReLU(),
        #     nn.MaxPool2d((4, 1),(4, 1)),
        #     Lambda(lambda x: x.view(x.size(0),-1)), # Reshape,
        #     nn.Sequential(Lambda(lambda x: x.view(1,-1) if 1==len(x.size()) else x ),nn.Linear(1500,1024)), # Linear,
        #     nn.BatchNorm1d(1024,1e-05,0.1,True),#BatchNorm1d,
        #     nn.ReLU(),
        #     nn.Dropout(0.3),
        #     nn.Sequential(Lambda(lambda x: x.view(1,-1) if 1==len(x.size()) else x ),nn.Linear(1024,512)), # Linear,
        #     nn.BatchNorm1d(1000,1e-05,0.1,True),#BatchNorm1d,
        #     nn.ReLU(),
        #     nn.Dropout(0.3),
        #     nn.Sequential(Lambda(lambda x: x.view(1,-1) if 1==len(x.size()) else x ),nn.Linear(512,167)), # Linear,
        #     nn.Sigmoid(),
        # )

    # print
    if should_log:
        print("got Nasa SA model of type {}".format(type(pretrained_model_reloaded_th)))

    # load the weights
    if state_dict is not None:
        pretrained_model_reloaded_th.load_state_dict(state_dict)

    # return
    return pretrained_model_reloaded_th

def generate_input_tensor(variant_list, file_twobit):
    # load the chromosome data
    # get the genome file
    hg19 = TwoBitFile(file_twobit)

    # loop for through the variants
    # for variant in variant_list:

    # get the chrom
    chromosome = hg19['chr11']
    position = 95311422

    # load the data
    ref_sequence, alt_sequence = dcc_basset_lib.get_ref_alt_sequences(position, 300, chromosome, 'C')

    print("got ref sequence one hot of type {} and shape {}".format(type(ref_sequence), len(ref_sequence)))
    print("got alt sequence one hot of type {} and shape {}".format(type(alt_sequence), len(alt_sequence)))

    # build list and transform into input
    sequence_list = []
    # sequence_list.append(ref_sequence)
    sequence_list.append(ref_sequence)
    # sequence_list.append(alt_sequence)
    sequence_list.append(alt_sequence)

    print(alt_sequence)

    # get the np array of right shape
    sequence_one_hot = dcc_basset_lib.get_one_hot_sequence_array(sequence_list)
    print("got sequence one hot of type {} and shape {}".format(type(sequence_one_hot), sequence_one_hot.shape))
    # print(sequence_one_hot)

    # create a pytorch tensor
    tensor = torch.from_numpy(sequence_one_hot)

    print("got pytorch tensor with type {} and shape {} and data type \n{}".format(type(tensor), tensor.shape, tensor.dtype))

    # build the input tensor
    tensor_initial = torch.unsqueeze(tensor, 3)
    tensor_input = tensor_initial.permute(0, 2, 1, 3)
    tensor_input = tensor_input.to(torch.float)

def split_variant(variant):
    pieces = variant.split(":")
    return pieces

def get_result_map(variant_list, result_tensor, label_list, debug = False):
    '''method to take a variant list, labels list and ML model result tensor
    and create a list of maps of the result for each variant'''

    # check that the dimensions match
    # should have tensor 2x as big as variant list (one result for ref, one for alt)
    if 2 * len(variant_list) != result_tensor.shape[0]:
        raise Exception("the result tensor should have 2x as many rows as the variant list (not {} tensor and {} variants)".format(result_tensor.shape[0], len(variant_list)))
    # the tensor results columns should match the size of the labels
    if len(label_list) != result_tensor.shape[1]:
        raise Exception("the result tensor should have as many columns as the label list (not {} tensor and {} labels)".format(result_tensor.shape[0]), len(label_list))

    # build the result list
    result_list = []

    # loop through the variants and add the resulting aggregated value in for each tissue
    for index, variant in enumerate(variant_list):
        result_map = {'var_id': variant}

        # calculate the aggregated result value
        # get the absolute value of the difference
        tensor_abs = torch.abs(result_tensor[index * 2] - result_tensor[index * 2 + 1])

        if debug:
            print("for variant {} got aggregated tensor \n{}".format(variant, tensor_abs))

        # add in the result for each tissue
        for index in range(0, len(label_list)):
            result_map[label_list[index]] = tensor_abs[index].item()

        # add the result to the list
        result_list.append(result_map)

    # return
    return result_list

def get_input_tensor_from_variant_list(variant_list, genome_lookup, region_size, debug = False):
    '''
    method to return the ML model input vectorcorresponding to the variant list
    '''

    # keep track of variants that return odd sequences
    variants_to_remove_list = []

    # list used to build the tensor
    sequence_list = []

    # loop through the variants
    for variant in variant_list:
        # split the variant
        variant_pieces = split_variant(variant)
        chrom = variant_pieces[0]
        position = int(variant_pieces[1])
        alt_allele = variant_pieces[3]

        # get the chrom
        chromosome_lookup = genome_lookup['chr' + chrom]

        # load the data
        ref_sequence, alt_sequence = get_ref_alt_sequences(position, int(region_size/2), chromosome_lookup, alt_allele)

        # verify the letters in the sequence
        try:
            for test_sequence in (ref_sequence, alt_allele):
                if not re.match("^[ACGT]*$", test_sequence):
                    # get letter N for unseuqnced regions; throw them out
                    # if re.match("[N].", test_sequence):
                    if debug:
                        print("for variant {} got incorrect letter in sequence \n{}".format(variant, test_sequence))
                    raise ValueError()
        except:
            variants_to_remove_list.append(variant)
            continue
            # else: 
            #     raise ValueError("for variant {} got incorrect letter in sequence \n{}".format(variant, test_sequence))

        # debug
        if debug:
            if len(alt_sequence) > region_size:
                print("Got long sequence for variant {}".format(variant))
            print("got ref sequence one hot of type {} and shape {}".format(type(ref_sequence), len(ref_sequence)))
            print("got alt sequence one hot of type {} and shape {}".format(type(alt_sequence), len(alt_sequence)))

        # append the two new regions to the sequence list
        sequence_list.append(ref_sequence)
        sequence_list.append(alt_sequence)

    # debug
    if debug:
        for index, seq in enumerate(sequence_list):
            print("({}) has size {}".format(index, len(seq)))
    
    # get the np array of right shape once all the variants have been added in
    sequence_one_hot = get_one_hot_sequence_array(sequence_list)
    if debug:
        print("got sequence one hot of type {} and shape {}".format(type(sequence_one_hot), sequence_one_hot.shape))
        # print(sequence_one_hot)

    # create a pytorch tensor
    tensor = torch.from_numpy(sequence_one_hot)

    if debug:
        print("got pytorch tensor with type {} and shape {} and data type \n{}".format(type(tensor), tensor.shape, tensor.dtype))

    # build the input tensor
    tensor_initial = torch.unsqueeze(tensor, 3)
    tensor_input = tensor_initial.permute(0, 2, 1, 3)
    tensor_input = tensor_input.to(torch.float)

    if debug:
        print("got transposed pytorch tensor with type {} and shape {} and data type \n{}".format(type(tensor_input), tensor_input.shape, tensor_input.dtype))

    # create updated list to return
    updated_variant_list = [variant for variant in variant_list if variant not in variants_to_remove_list]

    # return
    return updated_variant_list, tensor_input

if __name__ == '__main__':
    # set the data dir
    # dir_data = "/Users/mduby/Data/Broad/"
    dir_data = "/home/javaprog/Data/Broad/"

    # file locations
    file_input = dir_data + "Magma/Common/part-00011-6a21a67f-59b3-4792-b9b2-7f99deea6b5a-c000.csv"
    file_model_weights = dir_data + 'Basset/Model/pretrained_model_reloaded_th.pth'
    file_twobit = dir_data + 'Basset/TwoBitReader/hg19.2bit'


    # get the genome file
    hg19 = TwoBitFile(dir_data + 'Basset/TwoBitReader/hg19.2bit')

    print("two bit file of type {}".format(type(hg19)))

    # get the chrom
    chromosome = hg19['chr11']
    position = 95311422
    # chromosome = hg19['chr8']
    # position = 118184783

    print("two bit chromosome of type {}".format(type(chromosome)))

    # get the regular sequence
    ref_sequence = get_genomic_sequence(position, 3, chromosome)
    print("got ref sequence: {}".format(ref_sequence))

    # get the allele sequence
    alt_sequence = get_genomic_sequence(position, 3, chromosome, 'C')
    print("got alt sequence: {}".format(alt_sequence))
    print()

    # get ref and alt sequences
    ref_sequence, alt_sequence = get_ref_alt_sequences(position, 3, chromosome, 'C')
    print("got ref sequence: {}".format(ref_sequence))
    print("got alt sequence: {}".format(alt_sequence))
    print()

    sequence_list = []
    sequence_list.append(ref_sequence)
    sequence_list.append(alt_sequence)
    print("input sequence list {}".format(sequence_list))
    print()

    sequence_numpy = get_input_np_array(sequence_list)
    print("got sequence input of type {} and shape {} of\n{}".format(type(sequence_numpy), sequence_numpy.shape, sequence_numpy))
    print()

    sequence_one_hot = get_one_hot_sequence_array(sequence_list)
    print("got sequence one hot of type {} and shape {} of\n{}".format(type(sequence_one_hot), sequence_one_hot.shape, sequence_one_hot))
    print()

    # read the variant file
    # variant_file = dir_data + 'Magma/Common/part-00011-6a21a67f-59b3-4792-b9b2-7f99deea6b5a-c000.csv'
    variant_file = dir_data + 'dig-analysis-data/out/varianteffect/common/part-00000-24063efa-89ff-412d-9134-8cd90f09380b-c000.json'
    variant_list = get_variant_list(variant_file)
    for index in range(1, 10):
        print("got variant: {}".format(variant_list[index]))

    # # load the pytorch model
    # pretrained_model_reloaded_th = load_basset_model(file_model_weights)
    # pretrained_model_reloaded_th.eval()

    # # better summary of the model
    # print(pretrained_model_reloaded_th)

    # split a variant
    variant_id = "1:65359821:G:A"
    variant_pieces = split_variant(variant_id)
    print("got variant pieces: {}".format(variant_pieces))


    # test the reuls aggregating
    label_list = ['red', 'green', 'blue', 'yellow']
    variant_list = ['var1', 'var2', 'var3']
    result_tensor = torch.ones(6, 4)
    test_list = get_result_map(variant_list, result_tensor, label_list, True)
    print("for result aggregation test got: {}".format(test_list))

    # test the input tensor creation
    variant_list = ['1:65359821:G:A', '3:7359821:G:C', '8:3359821:G:T']
    variant_list, sequence_results = get_input_tensor_from_variant_list(variant_list, hg19, 20, True)
    print("got input tensor of type {} and shape {} and data \n{}".format(type(sequence_results), sequence_results.shape, sequence_results))

    # test for letter Ns
    variant_list.append("20:26319418:A:G")    
    variant_list, test_results = get_input_tensor_from_variant_list(variant_list, hg19, 600, False)
