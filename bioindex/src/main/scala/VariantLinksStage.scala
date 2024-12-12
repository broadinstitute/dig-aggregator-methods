package org.broadinstitute.dig.aggregator.methods.bioindex

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.config.emr._
import org.broadinstitute.dig.aws.emr._

/** The final result of all aggregator methods is building the BioIndex. All
  * outputs are to the dig-bio-index bucket in S3.
  */
class VariantLinksStage(implicit context: Context) extends Stage {
  val variantLinks = Input.Source.Dataset("annotated_regions/genetic_variant_effects/*/")

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(variantLinks)

  /** Rules for mapping input to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case variantLinks(project) => Outputs.Named(project)
  }

  /** Output to Job steps. */
  override def make(output: String): Job = {
    new Job(Job.PySpark(resourceUri("variantLinks.py"), s"--project=$output"))
  }
}
