package org.broadinstitute.dig.aggregator.methods.singlecell

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

class TranslateCellStateScoringStage(implicit context: Context) extends Stage {

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    masterVolumeSizeInGB = 100,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("bootstrap-scoring.sh")))
  )

  val liger: Input.Source = Input.Source.Raw("out/single_cell/staging/scoring/*/*/*/raw_cell_scoring.zip")

  override val sources: Seq[Input.Source] = Seq(liger)

  override val rules: PartialFunction[Input, Outputs] = {
    case liger(tissue, _, dataset) => Outputs.Named(s"$tissue/$dataset")
  }

  override def make(output: String): Job = {
    val flags: Seq[String] = output.split("/").toSeq match {
      case Seq(tissue, dataset) =>
        Seq(
          s"--tissue=$tissue",
          s"--dataset=$dataset")
    }
    new Job(Job.Script(resourceUri("translateCellState.py"), flags:_*))
  }
}
