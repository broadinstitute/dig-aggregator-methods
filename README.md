# DIG Aggregator Methods

This is the documentation about aggregator methods.

## Quickstart

Every sub-folder in this repo is a completely separate, independent method project for the [DIG Aggregator][core]. They can be run from here using one of the run scripts present in the root directory.

```bash
# dry run the bottom-line method
$ ./run.sh bottom-line

# run a stage of the bioindex method in test mode
$ ./run.sh bioindex --stage GenesStage --test --yes

# run a stage of the bioindex method in test mode from the SBT prompt
# this will include only t2d related phenotypes but will exclude the ones ending in adj (adjusted)
# --reprocess will force execution if already processed
# --clusters will override the default 5 clusters (good for tuning shell script jobs) to make better use of VM resources
sbt> run -c ../config..json --reprocess --clusters 10 --stage BassetStage --only T2D* --exclude *adj
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
