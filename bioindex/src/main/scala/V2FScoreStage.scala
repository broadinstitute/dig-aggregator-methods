package org.broadinstitute.dig.aggregator.methods.bioindex

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.config.emr._
import org.broadinstitute.dig.aws.emr._


class V2FScoreStage(implicit context: Context) extends Stage {
  val v2fScores = Input.Source.Dataset("annotated_regions/V2Fscores/*/")

  override val sources: Seq[Input.Source] = Seq(v2fScores)

  override val rules: PartialFunction[Input, Outputs] = {
    case v2fScores(_) => Outputs.Named("v2fScores")
  }

  /** Output to Job steps. */
  override def make(output: String): Job = {
    new Job(Job.PySpark(resourceUri("v2fScore.py")))
  }
}
