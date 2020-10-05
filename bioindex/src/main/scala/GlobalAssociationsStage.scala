package org.broadinstitute.dig.aggregator.methods.bioindex

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

/** The final result of all aggregator methods is building the BioIndex. All
  * outputs are to the dig-bio-index bucket in S3.
  */
class GlobalAssociationsStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val bottomLine = Input.Source.Success("out/metaanalysis/trans-ethnic/*/")

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(bottomLine)

  /** Rules for mapping input to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case bottomLine(phenotype) => Outputs.Named(phenotype)
  }

  /** Simple cluster with a little more memory. */
  override val cluster: ClusterDef = super.cluster.copy(
    masterInstanceType = Ec2.Strategy.generalPurpose(mem = 32.gb),
    slaveInstanceType = Ec2.Strategy.generalPurpose(mem = 32.gb)
  )

  /** Output to Job steps. */
  override def make(output: String): Job = {
    val globalScript = resourceUri("globalAssociations.py")
    val step         = Job.PySpark(globalScript, output)

    new Job(Seq(step))
  }
}
