package org.broadinstitute.dig.aggregator.methods.huge

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

class NearestGeneStage(implicit context: Context) extends Stage {

  val genes: Input.Source = Input.Source.Success("out/huge/geneidmap/genes/")
  val variants: Input.Source = Input.Source.Success("out/varianteffect/common/")

  override val sources: Seq[Input.Source] = Seq(genes, variants)

  override val cluster: ClusterDef = {
    super.cluster.copy(
      instances = 7
    )
  }

  override val rules: PartialFunction[Input, Outputs] = {
    case _ => Outputs.Named("NearestGene")
  }

  override def make(output: String): Job = {
    new Job(Job.PySpark(resourceUri("nearest-gene.py")))
  }
}
