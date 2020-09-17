package org.broadinstitute.dig.aggregator.methods.bioindex

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.config.emr._
import org.broadinstitute.dig.aws.emr._

/** The final result of all aggregator methods is building the BioIndex. All
  * outputs are to the dig-bio-index bucket in S3.
  */
class GeneAssociationsStage(implicit context: Context) extends Stage {
  val assoc = Input.Source.Dataset("gene_associations/*/*/")

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(assoc)

  /** Rules for mapping input to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case assoc(_, _) => Outputs.Named("associations")
  }

  /** Use latest EMR release. */
  override val cluster: ClusterDef = super.cluster.copy(
    releaseLabel = ReleaseLabel.emrLatest
  )

  /** Output to Job steps. */
  override def make(output: String): Job = {
    new Job(Job.PySpark(resourceUri("geneAssociations.py")))
  }
}
