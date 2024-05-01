package org.broadinstitute.dig.aggregator.methods.huge

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

class HugeCommonStage(implicit context: Context) extends Stage {

  val cache: Input.Source = Input.Source.Dataset("out/huge/cache/")
  val variants: Input.Source = Input.Source.Success("out/metaanalysis/bottom-line/trans-ethnic/*/")

  override val sources: Seq[Input.Source] = Seq(cache, variants)

  override val cluster: ClusterDef = {
    super.cluster.copy(
      instances = 3
    )
  }

  override val rules: PartialFunction[Input, Outputs] = {
    case cache() => Outputs.All
    case variants(phenotype) => Outputs.Named(phenotype)
  }

  override def make(output: String): Job = {
    new Job(Job.PySpark(resourceUri("huge-common.py"), s"--phenotype=$output"))
  }
}
