package org.broadinstitute.dig.aggregator.methods.geneassociations

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

object GeneAssociations extends Method {

  /** Add all stages used in this method here. Stages must be added in the
   * order they should be serially executed.
   */
  override def initStages(implicit context: Context) = {
    addStage(new Intake600TraitStage)
    addStage(new Combine600TraitStage)
    addStage(new CombineAssociationsStage)
  }
}
