package org.broadinstitute.dig.aggregator.methods.intake

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws.Ec2.Strategy
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

class TmpCompressionStage(implicit context: Context) extends Stage {

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("compression_bootstrap.sh"))),
    releaseLabel = ReleaseLabel("emr-6.7.0") // Need emr 6.1+ to write json files properly
  )

  val variants: Input.Source = Input.Source.Dataset("variants_processed/*/*/*/")

  override val sources: Seq[Input.Source] = Seq(variants)

  override val rules: PartialFunction[Input, Outputs] = {
    case variants(method, dataset, phenotype) => Outputs.Named(s"${method}/${dataset}/${phenotype}")
  }

  override def make(output: String): Job = {
    val tmpCompressionJob = resourceUri("tmpCompression.py")
    val methodDatasetPhenotype = output
    val steps = Seq(
      Job.PySpark(tmpCompressionJob, methodDatasetPhenotype)
    )

    new Job(steps)
  }
}
