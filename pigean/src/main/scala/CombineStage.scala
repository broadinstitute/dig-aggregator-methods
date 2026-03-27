package org.broadinstitute.dig.aggregator.methods.pigean

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

class CombineStage(implicit context: Context) extends Stage {

  override val cluster: ClusterDef = super.cluster.copy(
    masterVolumeSizeInGB = 500,
    instances = 1
  )

  val pigean: Input.Source = Input.Source.Success("out/pigean/staging/pigean/*/*/*/")

  override val sources: Seq[Input.Source] = Seq(pigean)

  override val rules: PartialFunction[Input, Outputs] = {
    case pigean(_, _, gene_set_size) => Outputs.Named(gene_set_size)
  }

  override def make(output: String): Job = {
    new Job(Seq(
      Job.Script(resourceUri("combineGS.py"), s"--gene-set-size=$output"),
      Job.Script(resourceUri("combineGSS.py"), s"--gene-set-size=$output")
    ), parallelSteps=true)
  }
}
