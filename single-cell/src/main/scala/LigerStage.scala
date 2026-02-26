package org.broadinstitute.dig.aggregator.methods.singlecell

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy

class LigerStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    masterInstanceType = Strategy.generalPurpose(vCPUs = 16),
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("bootstrap-liger.sh")))
  )

  val singleCell: Input.Source = Input.Source.Raw("single_cell/*/data.h5ad")

  override val sources: Seq[Input.Source] = Seq(singleCell)

  override val rules: PartialFunction[Input, Outputs] = {
    case singleCell(dataset) => Outputs.Named(dataset)
  }

  override def make(output: String): Job = {
    new Job(Job.Script(resourceUri("liger.py"), s"--dataset=$output"))
  }
}
