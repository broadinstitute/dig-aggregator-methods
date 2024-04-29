package org.broadinstitute.dig.aggregator.methods.geneassociations

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

class IntakeStage(implicit context: Context) extends Stage {
  override val cluster: ClusterDef = super.cluster.copy(
    applications = Seq.empty,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("intake_bootstrap.sh"))),
    instances = 1,
    stepConcurrency = 5
  )

  val raw: Input.Source = Input.Source.Raw("gene_associations_raw/*/*/*")

  override val sources: Seq[Input.Source] = Seq(raw)

  override val rules: PartialFunction[Input, Outputs] = {
    case raw(cohort, dataset, phenotype)  => Outputs.Named(s"$cohort/$dataset/$phenotype")
  }

  override def make(output: String): Job = {
    new Job(Job.Script(resourceUri("intake.py"), s"--path=$output"))
  }
}
