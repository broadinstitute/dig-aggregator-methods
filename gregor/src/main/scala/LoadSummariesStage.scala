package org.broadinstitute.dig.aggregator.methods.gregor

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws.emr._

/** Loads all the summaries produced by the GlobalEnrichmentStage.
  */
class LoadSummariesStage(implicit context: Context) extends Stage {
  val summary: Input.Source = Input.Source.Success("out/gregor/summary/*/")

  /** All the processors this processor depends on.
    */
  override val sources: Seq[Input.Source] = Seq(summary)

  /** All phenotypes are loaded together.
    */
  override val rules: PartialFunction[Input, Outputs] = {
    case summary(phenotype) => Outputs.Named("summaries")
  }

  /** Load them together in one shot.
    */
  override def make(output: String): Job = {
    new Job(Job.PySpark(resourceUri("loadSummaries.py")))
  }
}
