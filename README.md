# DIG Aggregator Methods

This is the documentation about aggregator methods.

## Quickstart

Every sub-folder in this repo is a completely separate, independent method project for the [DIG Aggregator][core]. They can be run from here using one of the run scripts present in the root directory.

```bash
# dry run the bottom-line method
$ ./run.sh bottom-line

# run a stage of the bioindex method in test mode
$ ./run.sh bioindex --stage GenesStage --test --yes
```

You can also simply go into the sub-directory of the method project and run it yourself.

## Creating a New Method

After installing [SBT][sbt], you can instantiate a new method from a [Giter8][g8] template in the root directory:

```bash
$ sbt new broadinstitute/dig-aggregator-method.g8
```

Simply enter the name of the new method and change any of the follow-up defaults as desired.

_NOTE: On the Broad file system, [SBT][sbt] can be installed with `use SBT`._


[sbt]: https://www.scala-sbt.org/
[core]: https://github.com/broadinstitute/dig-aggregator-core
[g8]: http://www.foundweekends.org/giter8/

## Data Locations

- phenotype stats
  - dataset specific
    - variants/{sequence tech}/{dataset}/{phenotype}/
  - ancestry specific
    - out/metaanalysis/ancestry-specific/{phenotype}/{ancestry}/
  - bottom line across all ancestries
    - out//metaanalysis/trans-ethnic/{phenotype}/
