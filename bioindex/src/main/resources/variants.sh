#!/bin/bash -xe
aws s3 sync "${JOB_BUCKET}/out/varianteffect/common/" "s3://dig-bio-index/variants/" --delete
