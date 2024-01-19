package org.broadinstitute.dig.aggregator.methods.bioindex

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

/** The final result of all aggregator methods is building the BioIndex. All
  * outputs are to the dig-bio-index bucket in S3.
  */
class TranscriptionsStage(implicit context: Context) extends Stage {
  val effects = Input.Source.Success("out/varianteffect/effects/")
  val motifs  = Input.Source.Dataset("transcription_factors/")

  /** Use memory-optimized machine with sizeable disk space for shuffling. */
  override val cluster: ClusterDef = super.cluster.copy(
    masterInstanceType = Ec2.Strategy.generalPurpose(),
    slaveInstanceType = Ec2.Strategy.generalPurpose(),
    instances = 6,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("cluster-bootstrap.sh")))
  )

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(effects, motifs)

  /** Rules for mapping input to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case effects() => Outputs.Named("cqs")
    case motifs()  => Outputs.Named("motifs")
  }

  /** Output to Job steps. */
  override def make(output: String): Job = {
    val script = output match {
      case "cqs"    => resourceUri("transcriptConsequences.py")
      case "motifs" => resourceUri("transcriptionFactors.py")
    }

    new Job(Job.PySpark(script))
  }
}
