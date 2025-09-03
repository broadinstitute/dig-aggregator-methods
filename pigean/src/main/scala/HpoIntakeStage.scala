package org.broadinstitute.dig.aggregator.methods.pigean

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

class HpoIntakeStage(implicit context: Context) extends Stage {

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("intake-bootstrap.sh")))
  )

  val binBucket: S3.Bucket = new S3.Bucket("dig-analysis-bin", None)
  val genes: Input.Source = Input.Source.Raw("hpo/phenotype_to_genes.txt", s3BucketOverride=Some(binBucket))

  override val sources: Seq[Input.Source] = Seq(genes)

  override val rules: PartialFunction[Input, Outputs] = {
    case genes() => Outputs.Named("orphanetIntake")
  }

  override def make(output: String): Job = {
    new Job(Job.Script(resourceUri("orphanetIntake.py")))
  }
}

