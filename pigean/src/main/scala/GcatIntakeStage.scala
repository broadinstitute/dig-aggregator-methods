package org.broadinstitute.dig.aggregator.methods.pigean

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

class GcatIntakeStage(implicit context: Context) extends Stage {

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("intake-bootstrap.sh")))
  )

  val binBucket: S3.Bucket = new S3.Bucket("dig-analysis-bin", None)
  val associations: Input.Source = Input.Source.Raw("gwas_catalog/gwas_catalog_associations.tsv", s3BucketOverride=Some(binBucket))

  override val sources: Seq[Input.Source] = Seq(associations)

  override val rules: PartialFunction[Input, Outputs] = {
    case associations() => Outputs.Named("gcatIntake")
  }

  override def make(output: String): Job = {
    new Job(Job.Script(resourceUri("gcatIntake.py")))
  }
}
