package org.broadinstitute.dig.aggregator.methods.singlecell

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

object SingleCell extends Method {

  override def initStages(implicit context: Context) = {
    addStage(new GenerateLigerBootstrapStage)
    addStage(new MakeH5adStage)
    addStage(new LigerStage)
    addStage(new TranslateLigerStage)
    addStage(new DownsampleStage)
    addStage(new FactorMatrixStage)
    addStage(new FactorPhewasStage)
    addStage(new TranslatePhewasStage)
    addStage(new RegressionStage)
    addStage(new PigeanStage)
    addStage(new TranslatePigeanStage)
    addStage(new GraphStage)
    addStage(new FactorStage)
  }
}
