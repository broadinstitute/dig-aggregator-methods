#!/usr/bin/python3

import json
import re
import sys

S3DIR = 's3://dig-analysis-data/out/varianteffect'


def rename_cqs_field(s):
    """
    Lifted from BioIndex code to camelCase the names of consequences.
    """
    s=re.sub(r'(?:[^a-z0-9]+)(.)', lambda m: m.group(1).upper(), s, flags=re.IGNORECASE)
    s=re.sub(r'^[A-Z][a-z]+', lambda m: m.group(0).lower(), s)

    return s


def exploded_consequences(row):
    """
    Extracts transcript consequences from the row.
    """
    for cqs in row.get('transcript_consequences', []):
        record = dict()

        # include the variant ID and locus with each consequence
        record['varId'] = row['id']
        record['chromosome'] = row['seq_region_name']
        record['position'] = int(row['start'])

        # apend all the consequence fields; ignore some
        for k, v in cqs.items():
            if k not in ['domains']:
                record[rename_cqs_field(k)] = v

        # add the record to the returned values
        yield record


for line in sys.stdin:
    row = json.loads(line)

    # write the common record to the output
    for cqs in exploded_consequences(row):
        json.dump(
            cqs,
            sys.stdout,
            separators=(',', ':'),
            check_circular=False,
        )

        # output a newline after the json
        sys.stdout.write('\n')




