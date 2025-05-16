package org.broadinstitute.dig.aggregator.methods.pigean

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

class CombineGSStage(implicit context: Context) extends Stage {

  override val cluster: ClusterDef = super.cluster.copy(
    masterVolumeSizeInGB = 100,
    instances = 1
  )

  val pigean: Input.Source = Input.Source.Success("out/pigean/staging/pigean/*/*/*/*/")

  override val sources: Seq[Input.Source] = Seq(pigean)

  override val rules: PartialFunction[Input, Outputs] = {
    case pigean(_, _, _, _) => Outputs.Named("combine")
  }

  override def make(output: String): Job = {
    new Job(Job.Script(resourceUri("combineGS.py")))
  }
}
