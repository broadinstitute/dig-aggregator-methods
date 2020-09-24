package org.broadinstitute.dig.aggregator.methods.bioindex

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

/** The final result of all aggregator methods is building the BioIndex. All
  * outputs are to the dig-bio-index bucket in S3.
  */
class PhewasAssociationsStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val bottomLine = Input.Source.Success("out/metaanalysis/trans-ethnic/*/")

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(bottomLine)

  /** Rules for mapping input to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case _ => Outputs.Named("phewas")
  }

  /** Cluster with a lots of memory to prevent shuffling. */
  override val cluster: ClusterDef = super.cluster.copy(
    masterInstanceType = Ec2.Strategy.generalPurpose(mem = 64.gb),
    slaveInstanceType = Ec2.Strategy.generalPurpose(mem = 64.gb),
    masterVolumeSizeInGB = 200,
    slaveVolumeSizeInGB = 200,
    instances = 6
  )

  /** Output to Job steps. */
  override def make(output: String): Job = {
    new Job(Job.PySpark(resourceUri("phewasAssociations.py")))
  }
}
