package org.broadinstitute.dig.aggregator.methods.bioindex

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.config.emr._
import org.broadinstitute.dig.aws.emr._


class ColocStage(implicit context: Context) extends Stage {
  val coloc = Input.Source.Dataset("coloc/*/")

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(coloc)

  /** Rules for mapping input to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case coloc(_) => Outputs.Named("coloc")
  }

  /** Output to Job steps. */
  override def make(output: String): Job = {
    new Job(Job.PySpark(resourceUri("coloc.py")))
  }
}
