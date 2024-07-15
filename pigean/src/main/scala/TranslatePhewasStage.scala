package org.broadinstitute.dig.aggregator.methods.pigean

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

class TranslatePhewasStage(implicit context: Context) extends Stage {

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1
  )

  val pigean: Input.Source = Input.Source.Success("out/pigean/staging/phewas/*/*/*/")

  override val sources: Seq[Input.Source] = Seq(pigean)

  override val rules: PartialFunction[Input, Outputs] = {
    case pigean(phenotype, sigma, geneSetSize) =>
      Outputs.Named(s"$phenotype/${sigma.split("=").last}/${geneSetSize.split("=").last}")
  }

  override def make(output: String): Job = {
    val flags: Seq[String] = output.split("/").toSeq match {
      case Seq(phenotype, sigma, geneSetSize) => Seq(s"--phenotype=$phenotype", s"--sigma=$sigma", s"--gene-set-size=$geneSetSize")
      case _ => throw new Exception("output must take form <phenotype>/<sigma>")
    }

    new Job(Job.Script(resourceUri("translatePhewas.py"), flags:_*))
  }
}
