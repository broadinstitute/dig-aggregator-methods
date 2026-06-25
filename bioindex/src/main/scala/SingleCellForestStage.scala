package org.broadinstitute.dig.aggregator.methods.bioindex

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.config.emr._
import org.broadinstitute.dig.aws.emr._

/** The final result of all aggregator methods is building the BioIndex. All
 * outputs are to the dig-bio-index bucket in S3.
 */
class SingleCellForestStage(implicit context: Context) extends Stage {
  val forestData = Input.Source.Raw("cross_dataset_test/all_gene_bulk_forestplot_endpoint_handoff.tsv")

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(forestData)

  /** Rules for mapping input to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case forestData() => Outputs.Named("forest")
  }

  /** Output to Job steps. */
  override def make(output: String): Job = {
    new Job(Job.PySpark(resourceUri("singleCellForest.py")))
  }
}
