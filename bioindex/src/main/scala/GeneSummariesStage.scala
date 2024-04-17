package org.broadinstitute.dig.aggregator.methods.bioindex

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

/** The final result of all aggregator methods is building the BioIndex. All
 * outputs are to the dig-bio-index bucket in S3.
 */
class GeneSummariesStage(implicit context: Context) extends Stage {
  val geneSummaries: Input.Source = Input.Source.Raw("genes/gpt_summaries/gene_summaries_gpt.json")

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1
  )

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(geneSummaries)

  /** Rules for mapping input to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case geneSummaries() => Outputs.Named("geneSummaries")
  }

  /** Output to Job steps. */
  override def make(output: String): Job = {
    new Job(Job.Script(resourceUri("geneSummaries.py")))
  }
}
