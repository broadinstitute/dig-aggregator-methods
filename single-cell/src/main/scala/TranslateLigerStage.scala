package org.broadinstitute.dig.aggregator.methods.singlecell

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

class TranslateLigerStage(implicit context: Context) extends Stage {

  val models = Seq("mouse_msigdb")

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1
  )

  val liger: Input.Source = Input.Source.Raw("out/single_cell/staging/liger/*/liger.zip")

  override val sources: Seq[Input.Source] = Seq(liger)

  override val rules: PartialFunction[Input, Outputs] = {
    case liger(dataset) => Outputs.Named(models.map { model =>
      s"$dataset/$model"
    }: _*)
  }

  override def make(output: String): Job = {
    val flags: Seq[String] = output.split("/").toSeq match {
      case Seq(dataset, model) =>
        Seq(
          s"--dataset=$dataset",
          s"--model=$model")
    }
    new Job(Job.Script(resourceUri("translateLiger.py"), flags:_*))
  }
}
