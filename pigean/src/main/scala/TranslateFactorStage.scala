package org.broadinstitute.dig.aggregator.methods.pigean

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

class TranslateFactorStage(implicit context: Context) extends Stage {

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("translate-bootstrap.sh")))
  )

  val pigean: Input.Source = Input.Source.Raw("out/pigean/staging/factor/*/*/*/f.out")

  override val sources: Seq[Input.Source] = Seq(pigean)

  override val rules: PartialFunction[Input, Outputs] = {
    case pigean(traitGroup, phenotype, model) =>
      Outputs.Named(s"$traitGroup/$phenotype/$model")
  }

  override def make(output: String): Job = {
    val flags: Seq[String] = output.split("/").toSeq match {
      case Seq(traitGroup, phenotype, model) =>
        Seq(s"--trait-group=$traitGroup", s"--phenotype=$phenotype", s"--model=$model")
      case _ => throw new Exception("output must take form <phenotype>/<sigma>")
    }

    new Job(Job.Script(resourceUri("translateFactor.py"), flags:_*))
  }
}
