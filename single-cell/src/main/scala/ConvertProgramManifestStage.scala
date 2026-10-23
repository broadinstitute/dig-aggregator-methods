package org.broadinstitute.dig.aggregator.methods.singlecell

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

class ConvertProgramManifestStage(implicit context: Context) extends Stage {

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("bootstrap-numpy.sh")))
  )

  val singleCell: Input.Source = Input.Source.Raw("out/single_cell/staging/factor_matrix/*/*/*")

  override val sources: Seq[Input.Source] = Seq(singleCell)

  override val rules: PartialFunction[Input, Outputs] = {
    case singleCell(dataset, cellType, _) => Outputs.Named(s"$dataset/$cellType")
  }

  override def make(output: String): Job = {
    val flags: Seq[String] = output.split("/").toSeq match {
      case Seq(dataset, cellType) =>
        Seq(
          s"--dataset=$dataset",
          s"--cell-type=$cellType"
        )
    }
    new Job(Job.Script(resourceUri("convertProgramManifest.py"), flags:_*))
  }
}
