from Bio import SeqIO
import json
import math
from scipy.stats import norm
from bioindexDB import BioIndexDB


class LineMaker:
    standardized_columns = ['chr', 'pos', 'ref', 'alt', 'p', 'or', 'beta', 'se', 'eaf', 'n']

    def __init__(self, header, fa_finder, f_out, metadata):
        self.metadata = metadata
        self.col_map = self.get_col_map(header)
        self.fa_finder = fa_finder
        self.f_out = f_out

    @staticmethod
    def normalize_chromosomes(value):
        if value == "23":
            return "X"
        elif value == "24":
            return "Y"
        elif value == "25":
            return "XY"
        elif value == "26" or value == "M" or value == "m":
            return "MT"
        else:
            return value

    @staticmethod
    def normalize_spaces(value):
        return value.strip().replace(' ', '_')

    @staticmethod
    def split_line(line):
        return [LineMaker.normalize_spaces(column) for column in line.split('\t')]

    @staticmethod
    def index_function(idx):
        return lambda line: line[idx]

    def get_col_map(self, header):
        return {column: idx for idx, column in enumerate(LineMaker.split_line(header))
                if column in self.standardized_columns}

    def make_lines(self, line):
        formatted_line = LineMaker.split_line(line)
        norm_chr = LineMaker.normalize_chromosomes(formatted_line[self.col_map['chr']])
        position = int(formatted_line[self.col_map['pos']])
        ref = formatted_line[self.col_map['ref']]
        alt = formatted_line[self.col_map['alt']]
        multiallelic = ',' in ref or ',' in alt
        line_builder = LineBuilder(norm_chr, position, formatted_line, self.col_map, self.metadata, multiallelic)
        for split_ref in ref.split(','):
            for split_alt in alt.split(','):
                actual_ref = self.fa_finder.get_actual_ref(norm_chr, position, split_ref)
                line = line_builder.build_line(split_ref, split_alt, actual_ref)
                if line is not None:
                    self.f_out.write('{}\n'.format(line.line_string()))


class LineBuilder:
    compliment = {
        'A': 'T',
        'T': 'A',
        'C': 'G',
        'G': 'C'
    }

    def __init__(self, norm_chr, position, formatted_line, col_map, metadata, multiallelic):
        self.norm_chr = norm_chr
        self.position = position
        self.formatted_line = formatted_line
        self.col_map = col_map
        self.metadata = metadata
        self.multiallelic = multiallelic

    def line(self, ref, alt, flip):
        return Line(
            self.norm_chr,
            self.position,
            ref,
            alt,
            self.formatted_line,
            self.col_map,
            self.metadata,
            self.multiallelic,
            flip
        )

    def build_line(self, ref, alt, actual_ref):
        compliment_ref = ''.join([self.compliment[base] for base in ref])
        compliment_alt = ''.join([self.compliment[base] for base in alt])
        is_ambiguous = alt == compliment_ref
        if not is_ambiguous:
            if ref == actual_ref:
                return self.line(ref, alt, flip=False)
            elif compliment_ref == actual_ref:
                return self.line(compliment_ref, compliment_alt, flip=False)
            elif alt == actual_ref:
                return self.line(alt, ref, flip=True)
            elif compliment_alt == actual_ref:
                return self.line(compliment_alt, compliment_ref, flip=True)
            #  TODO: add skipped lines to a central skipped file in s3
        if is_ambiguous:
            return None  # TODO: Ambiguous


class Line:
    def __init__(self, norm_chr, position, ref, alt, formatted_line, col_map, metadata, multiallelic, flip):
        self.formatted_line = formatted_line
        self.col_map = col_map
        self.norm_chr = norm_chr
        self.position = position
        self.ref = ref
        self.alt = alt
        self.metadata = metadata
        self.multiallelic = multiallelic
        self.flip = flip

        self.pValue = self.optional_float_column('p')
        self.oddsRatio = self.optional_float_column('or')
        self.beta = self.optional_float_column('beta')
        self.maf = None  # derived from eaf
        self.eaf = self.optional_float_column('eaf')
        self.stdErr = self.optional_float_column('se')
        self.zScore = None  # derived from beta and standard error
        self.n = self.optional_float_column('n')
        self.derive_optional_columns()

    def optional_float_column(self, column):
        if column in self.col_map:
            return float(self.formatted_line[self.col_map[column]])

    def infer_eff_n(self):
        if self.metadata['dichotomous']:
            return 4.0 / ((1.0 / self.metadata['cases']) + (1.0 / self.metadata['controls']))
        else:
            return self.metadata['subjects']

    def derive_optional_columns(self):
        if self.oddsRatio is None and self.beta is not None:
            self.oddsRatio = math.exp(self.beta)
        if self.beta is None and self.oddsRatio is not None:
            self.beta = math.log(self.oddsRatio)
        if self.stdErr is None and self.beta is not None and self.pValue is not None:
            self.stdErr = -abs(self.beta) / norm.ppf(self.pValue) if self.beta != 0.0 else 1.0
        if self.zScore is None and self.beta is not None and self.stdErr is not None:
            self.zScore = self.beta / self.stdErr
        if self.maf is None and self.eaf is not None:
            self.maf = self.eaf if self.eaf <= 0.5 else 1 - self.eaf
        if self.n is None:
            self.n = self.infer_eff_n()

    def line_string(self):
        return json.dumps({
            'varId': '{}:{}:{}:{}'.format(self.norm_chr, self.position, self.ref, self.alt),
            'chromosome': self.norm_chr,
            'position': self.position,
            'reference': self.ref,
            'alt': self.alt,
            'multiAllelic': self.multiallelic,
            'dataset': self.metadata['dataset'],
            'phenotype': self.metadata['phenotype'],
            'ancestry': self.metadata['ancestry'],
            'pValue': self.pValue,
            'beta': self.beta if not self.flip else -self.beta,
            'oddsRatio': self.oddsRatio if not self.flip else 1 / self.oddsRatio,
            'eaf': self.eaf if not self.flip else 1 - self.eaf,
            'maf': self.maf,
            'stdErr': self.stdErr,
            'zScore': self.zScore if not self.flip else -self.zScore,
            'n': self.n
        })


class FaFinder:
    def __init__(self):
        fasta_sequences = SeqIO.parse(open('data/Homo_sapiens.GRCh37.75.dna.primary_assembly.fa'), 'fasta')
        self.fa_dict = {fasta.id: str(fasta.seq) for fasta in fasta_sequences}

    def get_actual_ref(self, chr, position, ref):
        return self.fa_dict[chr][int(position) - 1:int(position) - 1 + len(ref)]


db = BioIndexDB()
fa_finder = FaFinder()

file = 'Antikainen2021_CADinT1D_EU.standardized'

metadata = {
    'dichotomous': False,  # TODO: Replace with DB call based on phenotype
    'subjects': 110,
    'cases': 10,
    'controls': 100,
    'dataset': 'GWAS_UKBiobank409k_eu',
    'phenotype': 'SBP',
    'ancestry': 'EU'
}
with open(f'data/{file}.tsv', 'r') as f_in:
    with open(f'data/{file}.json', 'w') as f_out:
        line_maker = LineMaker(f_in.readline(), fa_finder, f_out, metadata)
        for new_line in f_in.readlines():
            line_maker.make_lines(new_line)
