package org.broadinstitute.dig.aggregator.methods.ldsc

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

/** This is your aggregator method.
  *
  * All that needs to be done here is to implement the initStages function,
  * which adds stages to the method in the order they should be executed.
  *
  * When you are ready to run it, use SBT from the CLI:
  *
  *   sbt run [args]
  *
  * See the README of the dig-aggregator-core project for a complete list of
  * CLI arguments available.
  */
object LDScoreRegression extends Method {

  /** Add all stages used in this method here. Stages must be added in the
    * order they should be serially executed.
    */
  override def initStages(implicit context: Context) = {
    addStage(new PartitionRegionsStage)
    addStage(new MergeRegionsStage)
  }
}
