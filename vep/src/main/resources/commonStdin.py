import sys
import json

def dbSNP(v):
    """
    Extract the rsID for a given, colocated variant.
    """
    rsid = v.get('id')

    # only return if an actual rsID
    return rsid if rsid and rsid.startswith('rs') else None

def common_fields(row):
    """
    Extracts data from the colocated variant fields.
    """
    ref, alt = row['id'].split(':')[-2:]

    # find the correct colocated variant for this allele
    variant = colocated_variant(row, ref, alt)

    # no colocated variant found, just return the common data
    if not variant:
        return {
            'varId': row['id'],
            'consequence': row['most_severe_consequence'],
            'nearest': row['nearest'],
            'dbSNP': None,
            'minorAllele': None,
            'maf': None,
            'af': {
                'EU': None,
                'HS': None,
                'AA': None,
                'EA': None,
                'SA': None,
            },
        }

    return {
        'varId': row['id'],
        'consequence': row['most_severe_consequence'],
        'nearest': row['nearest'],
        'dbSNP': dbSNP(variant),
        'minorAllele': variant.get('minor_allele'),
        'maf': variant.get('minor_allele_freq'),
        'af': allele_frequencies(variant, ref, alt),
    }

def allele_frequencies(v, ref, alt):
    """
    Extract allele frequencies for a colocated variant.
    """
    af = v.get('frequencies', {})

    # find the matching allele in the frequency map (prefer alt)
    allele = next((a for a in [alt, ref] if a in af), None)

    # no allele frequency data?
    if not allele:
        return {
            'EU': None,
            'HS': None,
            'AA': None,
            'EA': None,
            'SA': None,
        }

    # lookup the frequency data
    def get_freq(*cols):
        freqs = [af[allele].get(c) for c in cols]

        # find the first non-null/zero frequency from the columns
        freq = next((f for f in freqs if f), None)

        # flip the frequency if this is the reference allele
        return 1.0 - freq if allele == ref else freq

    # try gnomad, if not there use 1kg
    return {
        'EU': get_freq('gnomad_nfe', 'eur'),
        'HS': get_freq('gnomad_amr', 'amr'),
        'AA': get_freq('gnomad_afr', 'afr'),
        'EA': get_freq('gnomad_eas', 'eas'),
        'SA': get_freq('gnomad_sas', 'sas'),
    }

def colocated_variant(row, ref, alt):
    """
    Find the first colocated variant with a matching allele string or None.
    """
    co = row.get('colocated_variants', [])

    # if there is only a single co-located variant, return it
    if len(co) == 1:
        return co[0]

    # only keep colocated variants where the minor allele is ref or alt
    variants = [v for v in co if v.get('minor_allele') in [ref, alt]]

    # fail if no colocated variants
    if not variants:
        return None

    # find the first with an rsID or default to the first variant
    return next((v for v in variants if v.get('id', '').startswith('rs')), variants[0])


for line in sys.stdin:
    row = json.loads(line)
    common = common_fields(row)
    json.dump(
        common,
        sys.stdout,
        separators=(',', ':'),
        check_circular=False,
    )
    sys.stdout.write('\n')

