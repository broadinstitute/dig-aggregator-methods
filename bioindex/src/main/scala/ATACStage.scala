package org.broadinstitute.dig.aggregator.methods.bioindex

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.config.emr._
import org.broadinstitute.dig.aws.emr._


class ATACStage(implicit context: Context) extends Stage {
  val atac = Input.Source.Dataset("annotated_regions/cis-regulatory_elements/*/")

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(atac)

  /** Rules for mapping input to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case atac(_) => Outputs.Named("atac")
  }

  /** Output to Job steps. */
  override def make(output: String): Job = {
    new Job(Job.PySpark(resourceUri("ATAC.py")))
  }
}
