package org.broadinstitute.dig.aggregator.methods.bioindex

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
object BioIndex extends Method {

  /** Add all stages used in this method here. Stages must be added in the
    * order they should be serially executed.
    */
  override def initStages(implicit context: Context) = {
    addStage(new GenesStage)
    addStage(new GeneVariantsStage)
    addStage(new VariantsStage)
    addStage(new AnnotatedRegionsStage)
    addStage(new GlobalEnrichmentStage)
    addStage(new TranscriptionsStage)
    addStage(new CredibleSetsStage)
    addStage(new ClumpedVariantsStage)
    addStage(new AssociationsStage)
    addStage(new TopAssociationsStage)
    addStage(new PhewasAssociationsStage)
    addStage(new DatasetAssociationsStage)
    addStage(new VariantAssociationsStage)
    addStage(new GeneAssociationsStage)
    addStage(new BurdenStage)
  }
}
