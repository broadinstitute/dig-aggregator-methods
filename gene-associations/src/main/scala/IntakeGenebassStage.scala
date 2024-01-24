package org.broadinstitute.dig.aggregator.methods.geneassociations

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

class IntakeGenebassStage(implicit context: Context) extends Stage {
  override val cluster: ClusterDef = super.cluster.copy(
    applications = Seq.empty,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("intake_bootstrap.sh"))),
    instances = 1,
    stepConcurrency = 10
  )

  val genebass: Input.Source = Input.Source.Raw("gene_associations_raw/genebass/*")

  override val sources: Seq[Input.Source] = Seq(genebass)

  override val rules: PartialFunction[Input, Outputs] = {
    case genebass(filename)  => Outputs.Named(filename)
  }

  override def make(output: String): Job = {
    new Job(Job.Script(resourceUri("genebassIntake.py"), s"--filename=$output"))
  }
}
