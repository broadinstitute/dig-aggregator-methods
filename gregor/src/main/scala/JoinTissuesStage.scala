package org.broadinstitute.dig.aggregator.methods.gregor

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws.emr.Job

/** All regions need to be joined with the tissue ontology and results of
  * GREGOR to be more useful.
  */
class JoinTissuesStage(implicit context: Context) extends Stage {
  val tissues: Input.Source = Input.Source.Success("tissues/")
  val regions: Input.Source = Input.Source.Success("out/gregor/regions/unsorted/")

  /** All the processors this processor depends on.
    */
  override val sources: Seq[Input.Source] = Seq(regions, tissues)

  /** Each phenotype is an output.
    */
  override val rules: PartialFunction[Input, Outputs] = {
    case _ => Outputs.Named("join")
  }

  /** Join regions with tissue ontology.
    */
  override def make(output: String): Job = {
    new Job(Job.PySpark(resourceUri("joinTissues.py")))
  }
}
