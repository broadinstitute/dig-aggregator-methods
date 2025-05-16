package org.broadinstitute.dig.aggregator.methods.pigean

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

class OrphanetIntakeStage(implicit context: Context) extends Stage {

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("intake-bootstrap.sh")))
  )

  val binBucket: S3.Bucket = new S3.Bucket("dig-analysis-bin", None)
  val classifications: Input.Source = Input.Source.Raw("orphanet/classifications/*.xml", s3BucketOverride=Some(binBucket))
  val genes: Input.Source = Input.Source.Raw("orphanet/genes/genes.xml", s3BucketOverride=Some(binBucket))

  override val sources: Seq[Input.Source] = Seq(classifications, genes)

  override val rules: PartialFunction[Input, Outputs] = {
    case classifications(_) => Outputs.Named("orphanetIntake")
    case genes() => Outputs.Named("orphanetIntake")
  }

  override def make(output: String): Job = {
    new Job(Job.Script(resourceUri("orphanetIntake.py")))
  }
}
