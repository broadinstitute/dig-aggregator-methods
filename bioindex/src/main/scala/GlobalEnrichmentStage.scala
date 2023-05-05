package org.broadinstitute.dig.aggregator.methods.bioindex

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

/** The final result of all aggregator methods is building the BioIndex. All
  * outputs are to the dig-bio-index bucket in S3.
  */
class GlobalEnrichmentStage(implicit context: Context) extends Stage {
  val enrichment = Input.Source.Success("out/ldsc/partitioned_heritability/")

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(enrichment)

  /** Use latest EMR release. */
  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1
  )

  /** Rules for mapping input to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case _ => Outputs.Named("enrichment")
  }

  /** Output to Job steps. */
  override def make(output: String): Job = {
    new Job(Job.PySpark(resourceUri("globalEnrichment.py")))
  }
}
