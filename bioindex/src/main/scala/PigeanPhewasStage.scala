package org.broadinstitute.dig.aggregator.methods.bioindex

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

class PigeanPhewasStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val phewas: Input.Source = Input.Source.Success("out/old-pigean/phewas/*/")

  override val sources: Seq[Input.Source] = Seq(phewas)

  override val rules: PartialFunction[Input, Outputs] = {
    case phewas(_) => Outputs.Named("pigeanPhewas")
  }

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1
  )

  /** Output to Job steps. */
  override def make(output: String): Job = {
    new Job(Job.PySpark(resourceUri("pigeanPhewas.py")))
  }
}
