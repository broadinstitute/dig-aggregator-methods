package org.broadinstitute.dig.aggregator.methods.bioindex

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.config.emr._
import org.broadinstitute.dig.aws.emr._

/** The final result of all aggregator methods is building the BioIndex. All
  * outputs are to the dig-bio-index bucket in S3.
  */
class geneExpressionStage(implicit context: Context) extends Stage {
  val geneExpression = Input.Source.Dataset("gene_expression_levels/*/")

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(geneExpression)

  /** Rules for mapping input to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case geneExpression(_) => Outputs.Named("expression")
  }

  /** Use latest EMR release. */
  override val cluster: ClusterDef = super.cluster.copy(
    releaseLabel = ReleaseLabel.emrLatest
  )

  /** Output to Job steps. */
  override def make(output: String): Job = {
    new Job(Job.PySpark(resourceUri("geneExpression.py")))
  }
}
