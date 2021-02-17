package org.broadinstitute.dig.aggregator.methods.bottomline

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy

/** After meta-analysis, this stage finds the most significant variant
  * every 50 kb across the entire genome.
  */
class TopAssociationsStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val clumps: Input.Source = Input.Source.Success("out/metaanalysis/clumped/*/")

  /** The output of meta-analysis is the input for top associations. */
  override val sources: Seq[Input.Source] = Seq(clumps)

  /** Process top associations for each phenotype. */
  override val rules: PartialFunction[Input, Outputs] = {
    case clumps(phenotype) => Outputs.Named(phenotype)
  }

  /** Simple cluster with more memory. */
  override val cluster: ClusterDef = super.cluster.copy(
    masterInstanceType = Strategy.generalPurpose(mem = 64.gb),
    instances = 1
  )

  /** Build the job. */
  override def make(output: String): Job = {
    val script    = resourceUri("topAssociations.py")
    val phenotype = output

    new Job(Job.PySpark(script, phenotype))
  }
}
