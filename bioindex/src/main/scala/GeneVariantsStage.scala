package org.broadinstitute.dig.aggregator.methods.bioindex

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.config.emr._
import org.broadinstitute.dig.aws.emr._

/** The final result of all aggregator methods is building the BioIndex. All
  * outputs are to the dig-bio-index bucket in S3.
  */
class GeneVariantsStage(implicit context: Context) extends Stage {
  val genes  = Input.Source.Dataset("genes/*/")
  val common = Input.Source.Success("out/varianteffect/common/")

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(genes, common)

  /** Rules for mapping input to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case _ => Outputs.Named("variants")
  }

  /** Use latest EMR release. */
  override val cluster: ClusterDef = super.cluster.copy(
    instances = 5
  )

  /** Output to Job steps. */
  override def make(output: String): Job = {
    new Job(Job.PySpark(resourceUri("geneVariants.py")))
  }
}
