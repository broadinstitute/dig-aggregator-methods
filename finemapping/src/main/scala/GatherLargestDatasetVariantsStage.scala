package org.broadinstitute.dig.aggregator.methods.finemapping

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy

/** final stage to collate all phenotype/ancestry results into single directory
  */
class GatherLargestDatasetVariantsStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val datasets: Input.Source = Input.Source.Success("out/finemapping/largest-datasets/*/")

  /** The output of gene pvalue analysis is the input for top results file. */
  override val sources: Seq[Input.Source] = Seq(datasets)

  /** Process top associations for each phenotype. */
  override val rules: PartialFunction[Input, Outputs] = {
    case datasets(phenotype) => Outputs.Named(phenotype)
  }

  /** Simple cluster with more memory. */
  /** override val cluster: ClusterDef = super.cluster.copy() */
  override val cluster: ClusterDef = super.cluster.copy(
    masterInstanceType = Strategy.generalPurpose(mem = 64.gb),
    slaveInstanceType = Strategy.generalPurpose(mem = 32.gb),
    instances = 4
  )

  /** Build the job. */
  override def make(output: String): Job = {
    val script    = resourceUri("gatherLargestDatasetVariants.py")
    val phenotype = output

    new Job(Job.PySpark(script, phenotype))
  }
}
