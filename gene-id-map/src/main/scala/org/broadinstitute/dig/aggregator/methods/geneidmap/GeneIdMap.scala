package org.broadinstitute.dig.aggregator.methods.geneidmap

import org.broadinstitute.dig.aggregator.core.{Context, Method}

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
object GeneIdMap extends Method {

  /** Add all stages used in this method here. Stages must be added in the
    * order they should be serially executed.
    */
  override def initStages(implicit context: Context): Unit = {
    addStage(new GeneIdMapStage)
  }
}
