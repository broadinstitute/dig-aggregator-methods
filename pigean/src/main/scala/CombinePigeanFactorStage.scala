package org.broadinstitute.dig.aggregator.methods.pigean

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

class CombinePigeanFactorStage(implicit context: Context) extends Stage {

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    stepConcurrency = 8
  )

  val geneFactor: Input.Source = Input.Source.Success("out/pigean/gene_factor/*/*/*/")
  val geneSetFactor: Input.Source = Input.Source.Success("out/pigean/gene_set_factor/*/*/*/")

  override val sources: Seq[Input.Source] = Seq(geneFactor, geneSetFactor)

  override val rules: PartialFunction[Input, Outputs] = {
    case geneFactor(sigmaPower, geneSetSize, phenotype) => Outputs.Named(
      s"$phenotype/${sigmaPower.split("sigma=").last}/${geneSetSize.split("size=").last}"
    )
    case geneSetFactor(sigmaPower, geneSetSize, phenotype) => Outputs.Named(
      s"$phenotype/${sigmaPower.split("sigma=").last}/${geneSetSize.split("size=").last}"
    )
  }

  override def make(output: String): Job = {
    val flags: Seq[String] = output.split("/").toSeq match {
      case Seq(phenotype, sigmaPower, geneSetSize) =>
        Seq(s"--phenotype=$phenotype", s"--sigma=$sigmaPower", s"--gene-set-size=$geneSetSize")
    }
    new Job(Job.Script(resourceUri("combinePigeanFactor.py"), flags:_*))
  }
}
