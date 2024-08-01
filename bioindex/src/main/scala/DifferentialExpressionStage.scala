package org.broadinstitute.dig.aggregator.methods.bioindex

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

class DifferentialExpressionStage(implicit context: Context) extends Stage {
  val datasets: Input.Source = Input.Source.Dataset("differential_expression/*/")

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1
  )

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(datasets)

  /** Rules for mapping input to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case datasets(_) => Outputs.Named("diff_exp")
  }

  /** Output to Job steps. */
  override def make(dataset: String): Job = {
    new Job(Job.PySpark(resourceUri("differentialExpression.py")))
  }
}
