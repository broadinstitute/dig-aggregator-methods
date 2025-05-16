package org.broadinstitute.dig.aggregator.methods.pigean

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

class MakeSumstatsStage(implicit context: Context) extends Stage {

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("sumstats-bootstrap.sh"))),
  )

  val transEthnic: Input.Source = Input.Source.Success("out/metaanalysis/bottom-line/trans-ethnic/*/")

  override val sources: Seq[Input.Source] = Seq(transEthnic)

  override val rules: PartialFunction[Input, Outputs] = {
    case transEthnic(phenotype) => Outputs.Named(phenotype)
  }

  override def make(output: String): Job = {
    new Job(Job.Script(resourceUri("makeSumstats.py"), s"--phenotype=$output"))
  }
}
