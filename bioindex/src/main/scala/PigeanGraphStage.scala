package org.broadinstitute.dig.aggregator.methods.bioindex

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

class PigeanGraphStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val graph: Input.Source = Input.Source.Success("out/pigean/graph/*/")

  override val sources: Seq[Input.Source] = Seq(graph)

  override val rules: PartialFunction[Input, Outputs] = {
    case graph(_) => Outputs.Named("pigeanGraph")
  }

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1
  )

  /** Output to Job steps. */
  override def make(output: String): Job = {
    new Job(Job.PySpark(resourceUri("pigeanGraph.py")))
  }
}
