package org.broadinstitute.dig.aggregator.methods.singlecell

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

class GraphStage(implicit context: Context) extends Stage {

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    stepConcurrency = 5,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("bootstrap-downsample.sh")))
  )

  val factorMatrix: Input.Source = Input.Source.Raw("out/single_cell/staging/factor_matrix/*/*/*/factor_matrix_factors.tsv")
  val runTypes: Seq[String] = Seq("norm", "metadata", "gene", "cell", "phewas", "regression", "pigean")

  override val sources: Seq[Input.Source] = Seq(factorMatrix)

  override val rules: PartialFunction[Input, Outputs] = {
    case factorMatrix(dataset, cellType, model) => Outputs.Named(runTypes.map { runType =>
      s"$dataset/$cellType/$model/$runType"
    }: _*)
  }

  override def make(output: String): Job = {
    val flags: Seq[String] = output.split("/").toSeq match {
      case Seq(dataset, cellType, model, runType) =>
        Seq(
          s"--dataset=$dataset",
          s"--cell-type=$cellType",
          s"--model=$model",
          s"--run-type=$runType"
        )
    }
    new Job(Job.Script(resourceUri("graph.py"), flags:_*))
  }
}
