package org.broadinstitute.dig.aggregator.methods.singlecell

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

class RegressionStage(implicit context: Context) extends Stage {

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("bootstrap-regression.sh")))
  )

  val factorMatrix: Input.Source = Input.Source.Raw("out/single_cell/staging/factor_matrix/*/*/*/factor_matrix_cell_loadings.tsv")

  override val sources: Seq[Input.Source] = Seq(factorMatrix)

  override val rules: PartialFunction[Input, Outputs] = {
    case factorMatrix(dataset, cellType, model) => Outputs.Named(s"$dataset/$cellType/$model")
  }

  override def make(output: String): Job = {
    val flags: Seq[String] = output.split("/").toSeq match {
      case Seq(dataset, cellType, model) =>
        Seq(
          s"--dataset=$dataset",
          s"--cell-type=$cellType",
          s"--model=$model")
    }
    new Job(Job.Script(resourceUri("regression.py"), flags:_*))
  }
}
