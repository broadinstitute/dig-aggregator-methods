package org.broadinstitute.dig.aggregator.methods.huge

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

class GeneIdMapStage(implicit context: Context) extends Stage {

  val binBucket: S3.Bucket = new S3.Bucket("dig-analysis-bin", None)
  val genes: Input.Source = Input.Source.Raw("genes/GRCh37/part-00000.json", s3BucketOverride=Some(binBucket))

  override val sources: Seq[Input.Source] = Seq(genes)

  override val cluster: ClusterDef = {
    super.cluster.copy(
      instances = 1
    )
  }

  override val rules: PartialFunction[Input, Outputs] = {
    case _ => Outputs.Named("GeneIdMap")
  }

  override def make(output: String): Job = {
    new Job(Job.PySpark(resourceUri("gene-id-map.py")))
  }
}
