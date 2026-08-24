package org.broadinstitute.dig.aggregator.methods.singlecell

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy

class GenerateLigerBootstrapStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    masterInstanceType = Strategy.memoryOptimized()
  )

  val binBucket: S3.Bucket = new S3.Bucket("dig-analysis-bin", None)
  val packages: Input.Source = Input.Source.Raw("single_cell/liger_packages/latest/liger-packages.zip", s3BucketOverride=Some(binBucket))

  override val sources: Seq[Input.Source] = Seq(packages)

  override val rules: PartialFunction[Input, Outputs] = {
    case packages() => Outputs.Named("generate")
  }

  override def make(output: String): Job = {
    new Job(Job.Script(resourceUri("generate-liger-bootstrap.sh")))
  }
}
