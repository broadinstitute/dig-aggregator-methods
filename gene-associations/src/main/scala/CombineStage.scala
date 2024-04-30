package org.broadinstitute.dig.aggregator.methods.geneassociations

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

class CombineStage(implicit context: Context) extends Stage {

  override val cluster: ClusterDef = super.cluster.copy(
    applications = Seq.empty,
    instances = 1,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("intake_bootstrap.sh"))),
    stepConcurrency = 5
  )

  val intake = Input.Source.Success("gene_associations/intake/*/*/*/")

  override val sources: Seq[Input.Source] = Seq(intake)

  override val rules: PartialFunction[Input, Outputs] = {
    case intake(phenotype, _, _) => Outputs.Named(phenotype)
  }

  override def make(output: String): Job = {
    new Job(Job.Script(resourceUri("combine.py"), s"--phenotype=$output"))
  }
}
