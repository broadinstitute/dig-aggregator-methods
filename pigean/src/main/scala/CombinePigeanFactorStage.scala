package org.broadinstitute.dig.aggregator.methods.pigean

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

class CombinePigeanFactorStage(implicit context: Context) extends Stage {

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    stepConcurrency = 8
  )

  val geneStats: Input.Source = Input.Source.Success("out/pigean/gene_stats/*/*/*/*/")
  val geneSetStats: Input.Source = Input.Source.Success("out/pigean/gene_set_stats/*/*/*/*/")

  override val sources: Seq[Input.Source] = Seq(geneStats, geneSetStats)

  override val rules: PartialFunction[Input, Outputs] = {
    case geneStats(sigmaPower, geneSetSize, traitGroup, phenotype) => Outputs.Named(
      s"$traitGroup/$phenotype/${sigmaPower.split("sigma=").last}/${geneSetSize.split("size=").last}"
    )
    case geneSetStats(sigmaPower, geneSetSize, traitGroup, phenotype) => Outputs.Named(
      s"$traitGroup/$phenotype/${sigmaPower.split("sigma=").last}/${geneSetSize.split("size=").last}"
    )
  }

  override def make(output: String): Job = {
    val flags: Seq[String] = output.split("/").toSeq match {
      case Seq(traitGroup, phenotype, sigmaPower, geneSetSize) =>
        Seq(s"--trait-group=$traitGroup", s"--phenotype=$phenotype", s"--sigma=$sigmaPower", s"--gene-set-size=$geneSetSize")
    }
    new Job(Job.Script(resourceUri("combinePigeanFactor.py"), flags:_*))
  }
}
