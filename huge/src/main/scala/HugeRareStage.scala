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
class HugeRareStage(implicit context: Context) extends Stage {

  class FilesForPhenotype(string: String, placeHolder: String = "<phenotype>") {
    def forPhenotype(phenotype: String): String = string.replaceAll(placeHolder, phenotype)
    def asGlob: String                          = string.replaceAll(placeHolder, "*")
  }

  val geneAssocFiles: FilesForPhenotype = new FilesForPhenotype("gene_associations/52k_*/<phenotype>/")
  val outDir: FilesForPhenotype         = new FilesForPhenotype("out/huge/rare/<phenotype>/")

  val geneAssociations: Input.Source = Input.Source.Dataset(geneAssocFiles.asGlob)

  /** Source inputs. */
  override val sources: Seq[Input.Source] = Seq(geneAssociations)

  /* Define settings for the cluster to run the job.
   */
  override val cluster: ClusterDef = {
    super.cluster.copy(
      instances = 3,
      bootstrapScripts = Seq(new BootstrapScript(resourceUri("huge-rare-bootstrap.sh"))),
      releaseLabel = ReleaseLabel("emr-6.7.0")
    )
  }

  /** Map inputs to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case geneAssociations(_, phenotype) => Outputs.Named(phenotype)
  }

  /** One job per phenotype (e.g. T2D)
    */
  override def make(output: String): Job = {
    val script    = resourceUri("huge-rare.py")
    val phenotype = output
    println(s"Making job with script $script for phenotype $phenotype.")
    val bucket = context.s3
    new Job(
      Job.PySpark(
        script,
        "--gene-associations",
        bucket.s3UriOf(geneAssocFiles.forPhenotype(phenotype)).toString,
        "--out-dir",
        bucket.s3UriOf(outDir.forPhenotype(phenotype)).toString
      )
    )
  }
}
