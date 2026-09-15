package org.broadinstitute.dig.aggregator.methods.singlecell

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

class FactorStage(implicit context: Context) extends Stage {

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1
  )

  val factors: Input.Source = Input.Source.Raw("out/single_cell/staging/factor_matrix/*/*/factor_matrix_factors.tsv")

  override val sources: Seq[Input.Source] = Seq(factors)

  override val rules: PartialFunction[Input, Outputs] = {
    case factors(dataset, cellType) => Outputs.Named(s"$dataset/$cellType")
  }

  override def make(output: String): Job = {
    val flags: Seq[String] = output.split("/").toSeq match {
      case Seq(dataset, cellType) =>
        Seq(
          s"--dataset=$dataset",
          s"--cell-type=$cellType")
    }
    new Job(Job.Script(resourceUri("runFactors.py"), flags:_*))
  }
}
