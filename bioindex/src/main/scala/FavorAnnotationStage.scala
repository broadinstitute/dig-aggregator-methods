package org.broadinstitute.dig.aggregator.methods.bioindex

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

/** The final result of all aggregator methods is building the BioIndex. All
  * outputs are to the dig-bio-index bucket in S3.
  */
class FavorAnnotationStage(implicit context: Context) extends Stage {


  /** Rules for mapping input to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case _ => Outputs.Named("favorAnnotations")
  }

  /** Output to Job steps. */
  override def make(output: String): Job = {
    new Job(Job.PySpark(resourceUri("favorAnnotations.py"))
  }
}
