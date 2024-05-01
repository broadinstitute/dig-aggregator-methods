package org.broadinstitute.dig.aggregator.methods.huge

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

class HugeCacheStage(implicit context: Context) extends Stage {

  val variantCommon: Input.Source = Input.Source.Success("out/varianteffect/common/")
  val nearestGenes: Input.Source = Input.Source.Success("out/huge/nearestgenes/")

  override val sources: Seq[Input.Source] = Seq(variantCommon, nearestGenes)

  override val cluster: ClusterDef = {
    super.cluster.copy(
      instances = 3
    )
  }

  override val rules: PartialFunction[Input, Outputs] = {
    case _ => Outputs.Named("HugeCache")
  }

  override def make(output: String): Job = {
    new Job(Job.PySpark(resourceUri("huge-cache.py")))
  }
}
