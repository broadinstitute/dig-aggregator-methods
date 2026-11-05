package org.broadinstitute.dig.aggregator.methods.falcon

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy

class GraphStage(implicit context: Context) extends Stage {

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("bootstrap_graph.sh"))),
    stepConcurrency = 8
  )

  val pigean: Input.Source = Input.Source.Raw("out/falcon/staging/eaggl/*/*/*/factors.out.gz")

  override val sources: Seq[Input.Source] = Seq(pigean)

  override val rules: PartialFunction[Input, Outputs] = {
    case pigean(traitGroup, phenotype, model) => Outputs.Named(
      s"$traitGroup/$phenotype/$model"
    )
  }

  override def make(output: String): Job = {
    val flags: Seq[String] = output.split("/").toSeq match {
      case Seq(traitGroup, phenotype, model) =>
        Seq(s"--trait-group=$traitGroup", s"--phenotype=$phenotype", s"--model=$model")
    }
    new Job(Job.Script(resourceUri("runGraph.py"), flags:_*))
  }
}
