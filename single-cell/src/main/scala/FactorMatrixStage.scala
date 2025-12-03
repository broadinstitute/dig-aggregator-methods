package org.broadinstitute.dig.aggregator.methods.singlecell

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

class FactorMatrixStage(implicit context: Context) extends Stage {

  val models = Seq("mouse_msigdb_phi1", "mouse_msigdb_phi5")

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("bootstrap-factor.sh")))
  )

  val downsample: Input.Source = Input.Source.Raw("out/single_cell/staging/downsample/*/*/norm_counts.sample.tsv.gz")

  override val sources: Seq[Input.Source] = Seq(downsample)

  override val rules: PartialFunction[Input, Outputs] = {
    case downsample(dataset, cellType) => Outputs.Named(models.map { model =>
      s"$dataset/$cellType/$model"
    }: _*)
  }

  override def make(output: String): Job = {
    val flags: Seq[String] = output.split("/").toSeq match {
      case Seq(dataset, cellType, model) =>
        Seq(
          s"--dataset=$dataset",
          s"--cell-type=$cellType",
          s"--model=$model")
    }
    new Job(Job.Script(resourceUri("factorMatrix.py"), flags:_*))
  }
}
