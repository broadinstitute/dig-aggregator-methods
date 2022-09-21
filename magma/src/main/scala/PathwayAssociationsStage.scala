package org.broadinstitute.dig.aggregator.methods.magma

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy

/** After meta-analysis, this stage finds the most significant variant
  * every 50 kb across the entire genome.
  */
class PathwayAssociationsStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

//  val associations: Input.Source = Input.Source.Success("out/magma/variant-associations/*/")
  val genes: Input.Source     = Input.Source.Success("out/magma/staging/genes/*/")

  /** Input sources. */
//  override val sources: Seq[Input.Source] = Seq(associations)
  override val sources: Seq[Input.Source] = Seq(genes)

  /** Process top associations for each phenotype. */
  override val rules: PartialFunction[Input, Outputs] = {
//    case associations(phenotype) => Outputs.Named(phenotype)
//    case genes()                 => Outputs.All

    case genes(phenotype)          => Outputs.Named(phenotype)
  }

  /** Simple cluster with more memory. */
  override val cluster: ClusterDef = super.cluster.copy(
    masterInstanceType = Strategy.computeOptimized(vCPUs = 32),
    instances = 1,
    masterVolumeSizeInGB = 80,
    applications = Seq.empty,
    stepConcurrency = 5,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("installMagma.sh")))
  )

  /** Build the job. */
  override def make(output: String): Job = {
    new Job(Job.Script(resourceUri("pathwayAssociations.sh"), output))
  }

  /** Before the jobs actually run, perform this operation.
    */
  override def prepareJob(output: String): Unit = {
    context.s3.rm(s"out/magma/staging/pathways/${output}/")
  }

  /** On success, write the _SUCCESS file in the output directory.
    */
  override def success(output: String): Unit = {
    context.s3.touch(s"out/magma/staging/pathways/${output}/_SUCCESS")
    ()
  }
}
