package org.broadinstitute.dig.aggregator.methods.singlecell

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

object SingleCell extends Method {

  override def initStages(implicit context: Context) = {
    addStage(new GenerateLigerBootstrapStage)
    addStage(new SplitByCellTypeStage)
    addStage(new ConvertCellStateManifestStage)
    addStage(new ConvertToMtxStage)
    addStage(new MakeH5adStage)
    addStage(new LigerStage)
    addStage(new NMFLigerStage)
    addStage(new TranslateLigerStage)
    addStage(new ConvertProgramManifestStage)
    addStage(new CellStateScoringStage)
    addStage(new BetasPhewasStage)
    addStage(new PigeanStage)
    addStage(new FactorStage)
    addStage(new TranslateCellStateScoringStage)
  }
}
