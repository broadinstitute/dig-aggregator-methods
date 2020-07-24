package org.broadinstitute.dig.aggregator.methods.bioindex

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

/** The final result of all aggregator methods is building the Bio Index. All
  * outputs are to the dig-bio-index bucket in S3.
  */
class RegionsStage(implicit context: Context) extends Stage {
  val regions    = Input.Source.Success("out/gregor/regions/joined/")
  val enrichment = Input.Source.Success("out/gregor/enrichment/*/")

  /** Use memory-optimized machine with sizeable disk space for shuffling. */
  override val cluster: ClusterDef = super.cluster.copy(
    masterInstanceType = Ec2.Strategy.memoryOptimized(),
    slaveInstanceType = Ec2.Strategy.memoryOptimized(),
    masterVolumeSizeInGB = 800,
    slaveVolumeSizeInGB = 800
  )

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(regions, enrichment)

  /** Rules for mapping input to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case regions()             => Outputs.Named("regions")
    case enrichment(phenotype) => Outputs.Named("globalEnrichment")
  }

  /** Output to Job steps. */
  override def make(output: String): Job = {
    val regionsScript    = resourceUri("regions/annotated.py")
    val enrichmentScript = resourceUri("regions/globalEnrichment.py")

    output match {
      case "globalEnrichment" => new Job(Job.PySpark(enrichmentScript))
      case "regions"          => new Job(Job.PySpark(regionsScript))
    }
  }
}
