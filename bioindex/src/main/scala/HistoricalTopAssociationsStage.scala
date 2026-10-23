package org.broadinstitute.dig.aggregator.methods.bioindex

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

/** The final result of all aggregator methods is building the BioIndex. All
 * outputs are to the dig-bio-index bucket in S3.
 */
class HistoricalTopAssociationsStage(implicit context: Context) extends Stage {
  val bioindexNames: Seq[String] = Seq(
    "20240621212625",
    "20241101192824",
    "20250727110719",
    "20251011144019",
    "20260325024233",
    "20260604230320",
    "current")
  val bucket: S3.Bucket = new S3.Bucket("dig-bio-index", None)
  val transEthnic: Input.Source = Input.Source.Success("associations/global/trans-ethnic/", s3BucketOverride = Some(bucket))
  val ancestrySpecific: Input.Source = Input.Source.Success("associations/global/ancestry/*/", s3BucketOverride = Some(bucket))

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(transEthnic, ancestrySpecific)

  /** Rules for mapping input to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case transEthnic() => Outputs.Named("TE")
    case ancestrySpecific(ancestry) => Outputs.Named(ancestry)
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
