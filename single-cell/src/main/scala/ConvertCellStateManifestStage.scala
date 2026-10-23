package org.broadinstitute.dig.aggregator.methods.singlecell

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

class ConvertCellStateManifestStage(implicit context: Context) extends Stage {

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1
  )

  val cellstate: Input.Source = Input.Source.Raw("curated_cell_states/*/*")

  override val sources: Seq[Input.Source] = Seq(cellstate)

  override val rules: PartialFunction[Input, Outputs] = {
    case cellstate(dataset, _) => Outputs.Named(dataset)
  }

  override def make(output: String): Job = {
    new Job(Job.Script(resourceUri("convertCellStateManifest.py"), s"--dataset=$output"))
  }
}
