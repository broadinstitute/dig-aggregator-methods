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

  val genesDir: String                  = "out/huge/geneidmap/genes/"
  val geneAssocFiles: FilesForPhenotype = new FilesForPhenotype("gene_associations/combined/<phenotype>/")
  val variantFiles: FilesForPhenotype   = new FilesForPhenotype("out/metaanalysis/bottom-line/trans-ethnic/<phenotype>/")
  val cacheDir: String                  = "out/huge/cache/"
  val outDir: FilesForPhenotype         = new FilesForPhenotype("out/huge/common/<phenotype>/")

  val genes: Input.Source            = Input.Source.Dataset(genesDir)
  val cache: Input.Source            = Input.Source.Dataset(cacheDir)
  val geneAssociations: Input.Source = Input.Source.Success(geneAssocFiles.asGlob)
  val variants: Input.Source         = Input.Source.Success(variantFiles.asGlob)

  /** Source inputs. */
  override val sources: Seq[Input.Source] = Seq(genes, geneAssociations, variants, cache)

  /* Define settings for the cluster to run the job.
   */
  override val cluster: ClusterDef = {
    super.cluster.copy(
      instances = 3
    )
  }

  /** Map inputs to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case genes()                        => Outputs.All
    case geneAssociations(phenotype) => Outputs.Named(phenotype)
    case variants(phenotype)            => Outputs.Named(phenotype)
  }

  /** One job per phenotype (e.g. T2D)
    */
  override def make(output: String): Job = {
    val script    = resourceUri("huge-common.py")
    val phenotype = output
    val bucket = context.s3
    new Job(
      Job.PySpark(
        script,
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
