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

  val genes: Input.Source = Input.Source.Success("out/magma/staging/genes/*/*/")

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(genes)

  /** Process top associations for each phenotype. */
  override val rules: PartialFunction[Input, Outputs] = {
    case genes(phenotype, ancestry) => Outputs.Named(s"$phenotype/${ancestry.split("=").last}")
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
    val jobInput = PathwaysAssociationsInput.fromOutput(output)
    new Job(Job.Script(resourceUri("pathwayAssociations.sh"), jobInput.phenotype, jobInput.ancestry))
  }

  /** Before the jobs actually run, perform this operation.
    */
  override def prepareJob(output: String): Unit = {
    val jobInput = PathwaysAssociationsInput.fromOutput(output)
    context.s3.rm(s"out/magma/staging/pathways/${jobInput.phenotype}/ancestry=${jobInput.ancestry}/")
  }

  /** On success, write the _SUCCESS file in the output directory.
    */
  override def success(output: String): Unit = {
    val jobInput = PathwaysAssociationsInput.fromOutput(output)
    context.s3.touch(s"out/magma/staging/pathways/${jobInput.phenotype}/ancestry=${jobInput.ancestry}/_SUCCESS")
    ()
  }
}

case class PathwaysAssociationsInput(
  phenotype: String,
  ancestry: String
)

case object PathwaysAssociationsInput {
  def fromOutput(output: String): PathwaysAssociationsInput = {
    output.split("/").toSeq match {
      case Seq(phenotype, ancestry) => PathwaysAssociationsInput(phenotype, ancestry)
    }
  }
}
