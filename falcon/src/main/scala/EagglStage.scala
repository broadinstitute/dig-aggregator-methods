package org.broadinstitute.dig.aggregator.methods.falcon

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy

class EagglStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("eaggl-bootstrap.sh"))),
    stepConcurrency = 8
  )

  val pigean: Input.Source = Input.Source.Raw("out/falcon/staging/falcon/*/*/*/pigean/*.gene_stats.tsv")

  override val sources: Seq[Input.Source] = Seq(pigean)

  override val rules: PartialFunction[Input, Outputs] = {
    case pigean(traitGroup, phenotype, model, _) => Outputs.Named(s"$traitGroup/$phenotype/$model")
  }

  override def make(output: String): Job = {
    val flags: Seq[String] = output.split("/").toSeq match {
      case Seq(traitGroup, phenotype, model) =>
        Seq(s"--trait-group=$traitGroup", s"--phenotype=$phenotype", s"--model=$model")
    }
    new Job(Job.Script(resourceUri("runEaggl.py"), flags: _*))
  }
}
