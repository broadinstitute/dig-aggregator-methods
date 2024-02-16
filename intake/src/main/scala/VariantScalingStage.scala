package org.broadinstitute.dig.aggregator.methods.intake

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

class VariantScalingStage(implicit context: Context) extends Stage {

  override val additionalResources: Seq[String] = Seq(
    "PortalDB.py"
  )

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 2,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("scaling_bootstrap.sh")))
  )

  val variants: Input.Source = Input.Source.Dataset("variants_qc/*/*/*/")

  override val sources: Seq[Input.Source] = Seq(variants)

  override val rules: PartialFunction[Input, Outputs] = {
    case variants(method, dataset, phenotype) => Outputs.Named(s"${method}/${dataset}/${phenotype}")
  }

  override def make(output: String): Job = {
    val steps = Seq(
      Job.PySpark(resourceUri("variantScaling.py"), output)
    )

    new Job(steps)
  }
}
