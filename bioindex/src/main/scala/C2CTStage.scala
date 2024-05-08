package org.broadinstitute.dig.aggregator.methods.bioindex

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

class C2CTStage(implicit context: Context) extends Stage {
  val c2ct: Input.Source = Input.Source.Success("out/credible_sets/specificity/*/*/*/")

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(c2ct)

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1
  )

  /** Rules for mapping input to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case c2ct(_, _, _) => Outputs.Named("c2ct")
  }

  /** Output to Job steps. */
  override def make(output: String): Job = {
    new Job(Job.PySpark(resourceUri("c2ct.py")))
  }
}
