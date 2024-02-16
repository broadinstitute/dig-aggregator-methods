package org.broadinstitute.dig.aggregator.methods.intake

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws.Ec2.Strategy
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._


class PreflightChecksStage(implicit context: Context) extends Stage {

  override val additionalResources: Seq[String] = Seq(
    "PortalDB.py"
  )

  override val cluster: ClusterDef = super.cluster.copy(
    applications = Seq.empty,
    instances = 1
  )

  val variantsMetadata: Input.Source = Input.Source.Dataset("variants_raw/*/*/*/")
  val variantsPassed: Input.Source = Input.Source.Success("variants_raw/*/*/*/")

  override val sources: Seq[Input.Source] = Seq(variantsMetadata, variantsPassed)

  override val rules: PartialFunction[Input, Outputs] = {
    case variantsMetadata(method, dataset, phenotype) => Outputs.Named(s"${method}/${dataset}/${phenotype}")
    case variantsPassed(method, dataset, phenotype) => Outputs.Null
  }

  override def make(output: String): Job = {
    new Job(Seq(Job.Script(resourceUri("preflightChecks.py"), s"--filepath=$output")))
  }
}
