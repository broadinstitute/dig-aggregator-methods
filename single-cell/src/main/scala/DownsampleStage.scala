package org.broadinstitute.dig.aggregator.methods.singlecell

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

class DownsampleStage(implicit context: Context) extends Stage {

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("bootstrap.sh")))
  )

  val singleCell: Input.Source = Input.Source.Raw("single_cell/*/dataset_metadata.json")

  override val sources: Seq[Input.Source] = Seq(singleCell)

  override val rules: PartialFunction[Input, Outputs] = {
    case singleCell(dataset) => Outputs.Named(dataset)
  }

  override def make(output: String): Job = {
    new Job(Job.Script(resourceUri("downsample.py"), s"--dataset=$output"))
  }
}
