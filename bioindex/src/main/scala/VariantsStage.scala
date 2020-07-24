package org.broadinstitute.dig.aggregator.methods.bioindex

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

/** The final result of all aggregator methods is building the Bio Index. All
  * outputs are to the dig-bio-index bucket in S3.
  */
class VariantsStage(implicit context: Context) extends Stage {
  val freq       = Input.Source.Success("out/frequencyanalysis/")
  val bottomLine = Input.Source.Success("out/metaanalysis/trans-ethnic/")
  val motifs     = Input.Source.Dataset("out/transcription_factors/")
  val common     = Input.Source.Success("out/varianteffect/common/")

  /** Use memory-optimized machine with sizeable disk space for shuffling. */
  override val cluster: ClusterDef = super.cluster.copy(
    masterInstanceType = Ec2.Strategy.memoryOptimized(),
    slaveInstanceType = Ec2.Strategy.memoryOptimized(),
    masterVolumeSizeInGB = 800,
    slaveVolumeSizeInGB = 800
  )

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(freq, bottomLine, motifs, common)

  /** Rules for mapping input to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case _ => Outputs.Named("variants")
  }

  /** Output to Job steps. */
  override def make(output: String): Job = {
    new Job(Job.PySpark(resourceUri("variants.py")))
  }
}
