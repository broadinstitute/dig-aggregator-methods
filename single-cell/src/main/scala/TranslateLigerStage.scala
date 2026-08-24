package org.broadinstitute.dig.aggregator.methods.singlecell

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

class TranslateLigerStage(implicit context: Context) extends Stage {

  val models = Seq("mouse_msigdb")

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("bootstrap-translate-liger.sh")))
  )

  val liger: Input.Source = Input.Source.Raw("out/single_cell/staging/liger/*/*/metadata.txt")

  override val sources: Seq[Input.Source] = Seq(liger)

  override val rules: PartialFunction[Input, Outputs] = {
    case liger(dataset, cellType) => Outputs.Named(models.map { model =>
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
    new Job(Job.Script(resourceUri("translateLiger.py"), flags:_*))
  }
}
