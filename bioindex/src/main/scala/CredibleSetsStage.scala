package org.broadinstitute.dig.aggregator.methods.bioindex

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

/** The final result of all aggregator methods is building the BioIndex. All
  * outputs are to the dig-bio-index bucket in S3.
  */
class CredibleSetsStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val variants = Input.Source.Dataset("credible_sets/*/*/")
  val regions  = Input.Source.Success("out/gregor/regions/joined/")

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(variants)

  /** Rules for mapping input to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case variants(_, phenotype) => Outputs.Named(phenotype)
    case regions()              => Outputs.All
  }

  /** Use memory-optimized machine with sizeable disk space for shuffling. */
  override val cluster: ClusterDef = super.cluster.copy(
    masterInstanceType = Ec2.Strategy.generalPurpose(mem = 64.gb),
    instances = 4
  )

  /** Output to Job steps. */
  override def make(output: String): Job = {
    val script    = resourceUri("credibleSets.py")
    val phenotype = output

    new Job(Job.PySpark(script, phenotype))
  }
}
