package org.broadinstitute.dig.aggregator.methods.bioindex

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.config.emr._
import org.broadinstitute.dig.aws.emr._

/** The final result of all aggregator methods is building the BioIndex. All
  * outputs are to the dig-bio-index bucket in S3.
  */
class GeneVariantsStage(implicit context: Context) extends Stage {
  val binBucket: S3.Bucket = new S3.Bucket("dig-analysis-bin", None)
  val genes: Input.Source = Input.Source.Raw("genes/GRCh37/part-00000.json", s3BucketOverride=Some(binBucket))
  val counts: Input.Source = Input.Source.Dataset("variant_counts/*/*/*/")

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(genes, counts)

  /** Rules for mapping input to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case counts(_) => Outputs.Named("variants")
    case genes() => Outputs.All
  }

  /** Use latest EMR release. */
  override val cluster: ClusterDef = super.cluster.copy(
    instances = 5
  )

  /** Output to Job steps. */
  override def make(output: String): Job = {
    new Job(Job.PySpark(resourceUri("geneVariants.py")))
  }
}
