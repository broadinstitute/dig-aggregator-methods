package org.broadinstitute.dig.aggregator.methods.bioindex

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

class MergedEvidenceStage(implicit context: Context) extends Stage {
  val mergedEvidence: Input.Source = Input.Source.Raw("merged_evidence/*")

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(mergedEvidence)

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1
  )

  /** Rules for mapping input to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case mergedEvidence(_) => Outputs.Named("mergedEvidence")
  }

  /** Output to Job steps. */
  override def make(output: String): Job = {
    new Job(Job.PySpark(resourceUri("mergedEvidence.py")))
  }
}
