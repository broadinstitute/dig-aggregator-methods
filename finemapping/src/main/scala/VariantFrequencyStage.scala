package org.broadinstitute.dig.aggregator.methods.finemapping

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy

/** Gather all variants with non maf of a certain limit
  */
class VariantFrequencyStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val commonFrequency: Input.Source = Input.Source.Success("out/varianteffect/common/")

  /** Ouputs to watch. */
  override val sources: Seq[Input.Source] = Seq(commonFrequency)

  /** Process top associations for each phenotype. */
  override val rules: PartialFunction[Input, Outputs] = {
    case commonFrequency() => Outputs.Named("VariantFrequencies")
  }

  /** Simple cluster with more memory. */
  override val cluster: ClusterDef = super.cluster.copy(
    masterInstanceType = Strategy.generalPurpose(mem = 64.gb),
    slaveInstanceType = Strategy.generalPurpose(mem = 32.gb),
    instances = 4
  )

  /** Build the job. */
  override def make(output: String): Job = {
    val script    = resourceUri("gatherVariantFrequencies.py")

    new Job(Job.PySpark(script))
  }
}