package org.broadinstitute.dig.aggregator.methods.intake

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws.Ec2.Strategy
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._


class VariantProcessingStage(implicit context: Context) extends Stage {

  override val additionalResources: Seq[String] = Seq(
    "PortalDB.py"
  )

  override val cluster: ClusterDef = super.cluster.copy(
    masterInstanceType = Strategy.memoryOptimized(),
    applications = Seq.empty,
    masterVolumeSizeInGB = 100,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("processor_bootstrap.sh"))),
    instances = 1
  )

  val variants: Input.Source = Input.Source.Dataset("variants_raw/*/*/*/")

  override val sources: Seq[Input.Source] = Seq(variants)

  override val rules: PartialFunction[Input, Outputs] = {
    case variants(method, dataset, phenotype) => Outputs.Named(s"${method}/${dataset}/${phenotype}")
  }

  override def make(output: String): Job = {
    // Extract gzip filname from s3
    val gz = ".*/([^/]*\\.gz)$".r
    val bgz = ".*/([^/]*\\.bgz)$".r
    val jobs = context.s3.ls(s"variants_raw/$output/").flatMap { keyObject =>
      keyObject.key match {
        case gz(filepath) => Some(Job.Script(resourceUri("variantProcessor.py"), s"--filepath=$output/$filepath"))
        case bgz(filepath) => Some(Job.Script(resourceUri("variantProcessor.py"), s"--filepath=$output/$filepath"))
        case _ => None
      }
    }

    new Job(jobs)
  }
}
