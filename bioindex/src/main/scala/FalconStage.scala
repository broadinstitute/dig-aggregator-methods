package org.broadinstitute.dig.aggregator.methods.bioindex

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

class FalconStage(implicit context: Context) extends Stage {
  val falconGenes: Input.Source = Input.Source.Raw("out/falcon/genes/*/falcon.genes")

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(falconGenes)

  /** Rules for mapping input to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case falconGenes(_) => Outputs.Named("falcon")
  }

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1
  )

  /** Output to Job steps. */
  override def make(output: String): Job = {
    new Job(Job.PySpark(resourceUri("falcon.py")))
  }
}
