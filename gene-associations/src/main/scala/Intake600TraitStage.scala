package org.broadinstitute.dig.aggregator.methods.geneassociations

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

class Intake600TraitStage(implicit context: Context) extends Stage {
  override val cluster: ClusterDef = super.cluster.copy(
    applications = Seq.empty,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("intake_bootstrap.sh")))
    instances = 1,
    stepConcurrency = 10
  )

  val traits600: Input.Source = Input.Source.Raw("gene_associations_raw/600k_600traits/*/*/*")

  override val sources: Seq[Input.Source] = Seq(traits600)

  override val rules: PartialFunction[Input, Outputs] = {
    case traits600(_, _, filename) if filename.nonEmpty  => Outputs.Named(filename)
    case _ => Outputs.Null
  }

  override def make(output: String): Job = {
    new Job(Job.Script(resourceUri("600TraitIntake.py"), s"--filename=$output"))
  }
}
