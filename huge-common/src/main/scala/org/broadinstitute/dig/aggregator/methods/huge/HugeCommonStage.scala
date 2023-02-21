package org.broadinstitute.dig.aggregator.methods.huge

import org.broadinstitute.dig.aggregator.core.{Context, Input, Outputs, Stage}
import org.broadinstitute.dig.aws.emr.{BootstrapScript, ClusterDef, Job, ReleaseLabel}

/** This is a stage in your method.
  *
  * Stages take one or more inputs and generate one or more outputs. Each
  * stage consists of a...
  *
  *   - list of input sources;
  *   - rules mapping inputs to outputs;
  *   - make function that returns a job used to produce a given output
  *
  * Optionally, a stage can also override...
  *
  *   - its name, which defaults to its class name
  *   - the cluster definition used to provision EC2 instances
  */
class HugeCommonStage(implicit context: Context) extends Stage {

  class FilesForPhenotype(string: String, placeHolder: String = "<phenotype>") {
    def forPhenotype(phenotype: String): String = string.replaceAll(placeHolder, phenotype)
    def asGlob: String                          = string.replaceAll(placeHolder, "*")
  }

//  "out/huge/cache/"
//  val geneFile: String                  = "genes/GRCh37/"
  val genesDir: String                  = "out/geneidmap/genes/"
  val geneAssocFiles: FilesForPhenotype = new FilesForPhenotype("gene_associations/52k_*/<phenotype>/")
  val variantFiles: FilesForPhenotype   = new FilesForPhenotype("out/metaanalysis/trans-ethnic/<phenotype>/")
  val cacheDir: String                  = "out/huge/cache/"
  val outDir: FilesForPhenotype         = new FilesForPhenotype("out/huge/common/<phenotype>/")

  val genes: Input.Source            = Input.Source.Dataset(genesDir)
  val cache: Input.Source            = Input.Source.Dataset(cacheDir)
  val geneAssociations: Input.Source = Input.Source.Dataset(geneAssocFiles.asGlob)
  val variants: Input.Source         = Input.Source.Success(variantFiles.asGlob)

  /** Source inputs. */
  override val sources: Seq[Input.Source] = Seq(genes, geneAssociations, variants, cache)

  /* Define settings for the cluster to run the job.
   */
  override val cluster: ClusterDef = {
    super.cluster.copy(
      instances = 3,
      releaseLabel = ReleaseLabel("emr-6.7.0")
    )
  }

  /** Map inputs to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case genes()                        => Outputs.All
    case geneAssociations(_, phenotype) => Outputs.Named(phenotype)
    case variants(phenotype)            => Outputs.Named(phenotype)
    case cache()                        => Outputs.All
  }

  /** One job per phenotype (e.g. T2D)
    */
  override def make(output: String): Job = {
    val script    = resourceUri("huge-common.py")
    val phenotype = output
    println(s"Making job with script $script for phenotype $phenotype.")
    val bucket = context.s3
    new Job(
      Job.PySpark(
        script,
        "--phenotype",
        phenotype,
        "--genes",
        bucket.s3UriOf(genesDir).toString,
        "--variants",
        bucket.s3UriOf(variantFiles.forPhenotype(phenotype)).toString,
        "--cache",
        bucket.s3UriOf(cacheDir).toString,
        "--out-dir",
        bucket.s3UriOf(outDir.forPhenotype(phenotype)).toString
      )
    )
  }
}
