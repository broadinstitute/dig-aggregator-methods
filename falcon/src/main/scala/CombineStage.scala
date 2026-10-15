package org.broadinstitute.dig.aggregator.methods.falcon

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy

class CombineStage(implicit context: Context) extends Stage {

  override val cluster: ClusterDef = super.cluster.copy(
    masterVolumeSizeInGB = 500,
    instances = 1
  )

  val pigean: Input.Source = Input.Source.Success("out/falcon/staging/pigean/*/*/*/")

  override val sources: Seq[Input.Source] = Seq(pigean)

  override val rules: PartialFunction[Input, Outputs] = {
    case pigean(_, _, model) => Outputs.Named(model)
  }

  override def make(output: String): Job = {
    new Job(Seq(
      Job.Script(resourceUri("combineGS.py"), s"--model=$output"),
      Job.Script(resourceUri("combineGSS.py"), s"--modele=$output")
    ), parallelSteps=true)
  }
}
