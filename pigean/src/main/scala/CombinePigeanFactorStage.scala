package org.broadinstitute.dig.aggregator.methods.pigean

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

class CombinePigeanFactorStage(implicit context: Context) extends Stage {

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    stepConcurrency = 8
  )

  val geneStats: Input.Source = Input.Source.Success("out/pigean/gene_stats/*/*/*/")
  val geneSetStats: Input.Source = Input.Source.Success("out/pigean/gene_set_stats/*/*/*/")

  override val sources: Seq[Input.Source] = Seq(geneStats, geneSetStats)

  override val rules: PartialFunction[Input, Outputs] = {
    case geneStats(geneSetSize, traitGroup, phenotype) => Outputs.Named(
      s"$traitGroup/$phenotype/$geneSetSize"
    )
    case geneSetStats(geneSetSize, traitGroup, phenotype) => Outputs.Named(
      s"$traitGroup/$phenotype/$geneSetSize"
    )
  }

  override def make(output: String): Job = {
    val flags: Seq[String] = output.split("/").toSeq match {
      case Seq(traitGroup, phenotype, geneSetSize) =>
        Seq(s"--trait-group=$traitGroup", s"--phenotype=$phenotype", s"--gene-set-size=$geneSetSize")
    }
    new Job(Job.Script(resourceUri("combinePigeanFactor.py"), flags:_*))
  }
}
