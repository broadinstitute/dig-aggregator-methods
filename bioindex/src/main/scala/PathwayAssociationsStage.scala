package org.broadinstitute.dig.aggregator.methods.bioindex

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.config.emr._
import org.broadinstitute.dig.aws.emr._

/** The final result of all aggregator methods is building the BioIndex. All
  * outputs are to the dig-bio-index bucket in S3.
  */
class PathwayAssociationsStage(implicit context: Context) extends Stage {
  val pathways = Input.Source.Success("out/magma/pathway-associations/*/")

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(pathways)

  /** Rules for mapping input to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case pathways(phenotype) => Outputs.Named("pathways")
  }

  /** Output to Job steps. */
  override def make(output: String): Job = {
    val script = resourceUri("pathwayAssociations.py")

    new Job(Job.PySpark(script))
  }
}
