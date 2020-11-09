package org.broadinstitute.dig.aggregator.methods.burdenbinning

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

/** This is a stage in your method.
  *
  * Stages take one or more inputs and generate one or more outputs. Each
  * stage consists of a...
  *
  *   - list of input sources;
  *   - rules mapping inputs to outputs;
  *   - make function that returns a job used to produce a given output
  *
  * Optionally, a stage can also override...
  *
  *   - its name, which defaults to its class name
  *   - the cluster definition used to provision EC2 instances
  */
class BurdenbinningStage(implicit context: Context) extends Stage {
 import MemorySize.Implicits._

  val variantEffects: Input.Source = Input.Source.Success("out/varianteffect/effects/")

  /** The output of variant effects is the input for burden binning results file. */
  override val sources: Seq[Input.Source] = Seq(variantEffects)

  /** Process burden binning associations. */
  override val rules: PartialFunction[Input, Outputs] = {
    case variantEffects() => Outputs.Named("BurdenBinning")
  }

  /** Simple cluster with more memory. */
  override val cluster: ClusterDef = super.cluster.copy(
    masterInstanceType = Strategy.generalPurpose(mem = 64.gb),
//    slaveInstanceType = Strategy.generalPurpose(mem = 32.gb),
//    instances = 4
  )

  /** Build the job. */
  override def make(output: String): Job = {
    val script    = resourceUri("burdenBinning.py")

    new Job(Job.PySpark(script))
  }
}
