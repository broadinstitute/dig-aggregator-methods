package org.broadinstitute.dig.aggregator.methods.singlecell

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy

class BetasPhewasStage(implicit context: Context) extends Stage {

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    masterVolumeSizeInGB = 100,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("bootstrap-scoring.sh")))
  )

  val singleCell: Input.Source = Input.Source.Raw("out/single_cell/staging/scoring/*/*/*/*")

  override val sources: Seq[Input.Source] = Seq(singleCell)

  override val rules: PartialFunction[Input, Outputs] = {
    case singleCell(tissue, cellType, dataset, _) => Outputs.Named(s"$tissue/$cellType/$dataset")
  }

  override def make(output: String): Job = {
    val flags: Seq[String] = output.split("/").toSeq match {
      case Seq(tissue, cellType, dataset) =>
        Seq(
          s"--tissue=$tissue",
          s"--cell-type=$cellType",
          s"--dataset=$dataset"
        )
    }
    new Job(Job.Script(resourceUri("betasPhewas.py"), flags:_*))
  }
}
