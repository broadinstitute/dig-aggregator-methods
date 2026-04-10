package org.broadinstitute.dig.aggregator.methods.singlecell

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy

class MakeH5adStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    masterInstanceType = Strategy.memoryOptimized(128.gb),
    masterVolumeSizeInGB = 100,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("bootstrap-h5ad.sh")))
  )

  val singleCell: Input.Source = Input.Source.Raw("single_cell/*/dataset_metadata.json")

  override val sources: Seq[Input.Source] = Seq(singleCell)

  override val rules: PartialFunction[Input, Outputs] = {
    case singleCell(dataset) => Outputs.Named(dataset)
  }

  override def make(output: String): Job = {
    new Job(Job.Script(resourceUri("makeH5ad.py"), s"--dataset=$output"))
  }
}
