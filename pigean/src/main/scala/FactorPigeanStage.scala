package org.broadinstitute.dig.aggregator.methods.pigean

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

class FactorPigeanStage(implicit context: Context) extends Stage {

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("pigean-bootstrap.sh"))),
    stepConcurrency = 8
  )

  val pigean: Input.Source = Input.Source.Success("out/pigean/staging/pigean/*/*/*/*/")

  override val sources: Seq[Input.Source] = Seq(pigean)

  override val rules: PartialFunction[Input, Outputs] = {
    case pigean(traitGroup, phenotype, sigmaPower, geneSetSize) => Outputs.Named(
      s"${traitGroup}/$phenotype/${sigmaPower.split("sigma=").last}/${geneSetSize.split("size=").last}"
    )
  }

  override def make(output: String): Job = {
    val flags: Seq[String] = output.split("/").toSeq match {
      case Seq(traitGroup, phenotype, sigmaPower, geneSetSize) =>
        Seq(s"--trait-group=$traitGroup", s"--phenotype=$phenotype", s"--sigma=$sigmaPower", s"--gene-set-size=$geneSetSize")
    }
    new Job(Job.Script(resourceUri("factorPigean.py"), flags:_*))
  }
}
