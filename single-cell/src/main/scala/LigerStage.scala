package org.broadinstitute.dig.aggregator.methods.singlecell

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy

class LigerStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    masterInstanceType = Strategy.memoryOptimized(),
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("bootstrap-liger.sh")))
  )

  val singleCell: Input.Source = Input.Source.Raw("out/single_cell/staging/h5ad/*/*.h5ad")

  override val sources: Seq[Input.Source] = Seq(singleCell)

  override val rules: PartialFunction[Input, Outputs] = {
    case singleCell(dataset, cellType) => Outputs.Named(s"$dataset/$cellType")
  }

  override def make(output: String): Job = {
    val flags: Seq[String] = output.split("/").toSeq match {
      case Seq(dataset, cellType) =>
        Seq(
          s"--dataset=$dataset",
          s"--cell-type=$cellType"
        )
    }
    new Job(Job.Script(resourceUri("liger.py"), flags:_*))
  }
}
