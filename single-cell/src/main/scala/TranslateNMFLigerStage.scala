package org.broadinstitute.dig.aggregator.methods.singlecell

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

class TranslateNMFLigerStage(implicit context: Context) extends Stage {

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("bootstrap-translate-liger.sh")))
  )

  val liger: Input.Source = Input.Source.Raw("out/single_cell/staging/nmf/liger/*/*/*/factor_report.txt")

  override val sources: Seq[Input.Source] = Seq(liger)

  override val rules: PartialFunction[Input, Outputs] = {
    case liger(tissue, cellType, dataset) => Outputs.Named(s"$tissue/$cellType/$dataset")
  }

  override def make(output: String): Job = {
    val flags: Seq[String] = output.split("/").toSeq match {
      case Seq(tissue, cellType, dataset) =>
        Seq(
          s"--tissue=$tissue",
          s"--dataset=$dataset",
          s"--cell-type=$cellType"
        )
    }
    new Job(Job.Script(resourceUri("translateNMFLiger.py"), flags:_*))
  }
}
