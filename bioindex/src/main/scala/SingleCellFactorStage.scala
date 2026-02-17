package org.broadinstitute.dig.aggregator.methods.bioindex

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

class SingleCellFactorStage(implicit context: Context) extends Stage {

  val factors: Input.Source = Input.Source.Raw("out/single_cell/factors/*/*/*/*.json")

  override val sources: Seq[Input.Source] = Seq(factors)

  override val rules: PartialFunction[Input, Outputs] = {
    case factors(_, _, _, _) => Outputs.Named("singleCellFactor")
  }

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1
  )

  /** Output to Job steps. */
  override def make(output: String): Job = {
    new Job(Job.PySpark(resourceUri("singleCellFactor.py")))
  }
}
