package org.broadinstitute.dig.aggregator.methods.singlecell

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy

class MakeH5adStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    masterInstanceType = Strategy.memoryOptimized(mem = 128.gb),
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("bootstrap-h5ad.sh")))
  )

  val singleCell: Input.Source = Input.Source.Raw("out/single_cell/staging/split/*/*/*")

  override val sources: Seq[Input.Source] = Seq(singleCell)

  override val rules: PartialFunction[Input, Outputs] = {
    case singleCell(dataset, cellType, _) => Outputs.Named(s"$dataset/$cellType")
  }

  override def make(output: String): Job = {
    val flags: Seq[String] = output.split("/").toSeq match {
      case Seq(dataset, cellType) =>
        Seq(
          s"--dataset=$dataset",
          s"--cell-type=$cellType")
    }
    new Job(Job.Script(resourceUri("makeH5ad.py"), flags:_*))
  }
}
