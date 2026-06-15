package org.broadinstitute.dig.aggregator.methods.bioindex

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

/** The final result of all aggregator methods is building the BioIndex. All
 * outputs are to the dig-bio-index bucket in S3.
 */
class HistoricalTopAssociationsStage(implicit context: Context) extends Stage {
  val bioindexNames: Seq[String] = Seq("dig-bio-index-20251011144019", "dig-bio-index-20260325024233", "dig-bio-index-20260604230320", "dig-bio-index")
  val buckets: Seq[S3.Bucket] = bioindexNames.map(bioindexName => new S3.Bucket(bioindexName, None))
  val transEthnic: Seq[Input.Source] = buckets.map { bucket =>
    Input.Source.Success("associations/global/trans-ethnic/", s3BucketOverride = Some(bucket))
  }
  val ancestrySpecific: Seq[Input.Source] = buckets.map { bucket =>
    Input.Source.Success("associations/global/ancestry/*/", s3BucketOverride = Some(bucket))
  }

  /** Input sources. */
  override val sources: Seq[Input.Source] = transEthnic ++ ancestrySpecific

  /** Rules for mapping input to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case transEthnic.head() => Outputs.Named("TE")
    case ancestrySpecific.head(ancestry) => Outputs.Named(ancestry)
  }

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1
  )

  /** Output to Job steps. */
  override def make(output: String): Job = {
    val bioindices = bioindexNames.mkString(",")
    new Job(Job.PySpark(resourceUri("historicalTopAssociations.py"), s"--bioindices=$bioindices", s"--ancestry=$output"))
  }
}
