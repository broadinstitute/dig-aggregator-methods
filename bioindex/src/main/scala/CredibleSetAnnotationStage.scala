package org.broadinstitute.dig.aggregator.methods.bioindex

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._


class CredibleSetAnnotationStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val annotations: Input.Source = Input.Source.Success("out/ldsc/regions/credible_sets/*/*/")

  override val sources: Seq[Input.Source] = Seq(annotations)

  override val rules: PartialFunction[Input, Outputs] = {
    case annotations(_, _) => Outputs.Named("annotations")
  }

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1
  )

  override def make(output: String): Job = {
    new Job(Job.PySpark(resourceUri("credibleSetAnnotations.py")))
  }
}
