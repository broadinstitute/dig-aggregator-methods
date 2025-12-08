package org.broadinstitute.dig.aggregator.methods.singlecell

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

object SingleCell extends Method {

  override def initStages(implicit context: Context) = {
    addStage(new DownsampleStage)
    addStage(new FactorMatrixStage)
    addStage(new FactorPhewasStage)
    addStage(new RegressionStage)
  }
}
