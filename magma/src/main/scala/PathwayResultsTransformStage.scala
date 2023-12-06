package org.broadinstitute.dig.aggregator.methods.magma

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy

/** After meta-analysis, this stage finds the most significant variant
  * every 50 kb across the entire genome.
  */
class PathwayResultsTransformStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val pathways: Input.Source = Input.Source.Success("out/magma/staging/pathways/*/")

  /** The output of gene pvalue analysis is the input for top results file. */
  override val sources: Seq[Input.Source] = Seq(pathways)

  /** Process top associations for each phenotype. */
  override val rules: PartialFunction[Input, Outputs] = {
    case pathways(phenotype) => Outputs.Named(phenotype)
  }

  /** Simple cluster with more memory. */
  override val cluster: ClusterDef = super.cluster.copy(
    applications = Seq.empty,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("installTransformPackages.sh"))),
    instances = 1,
    releaseLabel = ReleaseLabel("emr-6.7.0")
  )

  /** Build the job. */
  override def make(output: String): Job = {
    val script    = resourceUri("pathwayResultsTransform.py")
    val phenotype = output

    new Job(Job.Script(script, s"--phenotype=$phenotype"))
  }

  /** Before the jobs actually run, perform this operation.
   */
  override def prepareJob(output: String): Unit = {
    context.s3.rm(s"out/magma/pathway-associations/$output/")
  }

  /** On success, write the _SUCCESS file in the output directory.
   */
  override def success(output: String): Unit = {
    context.s3.touch(s"out/magma/pathway-associations/$output/_SUCCESS")
    ()
  }
}
