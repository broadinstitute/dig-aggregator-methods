package org.broadinstitute.dig.aggregator.methods.gregor

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws.emr._

class JoinTissuesStage(implicit context: Context) extends Stage {
  val regions: Input.Source = Input.Source.Dataset("annotated_regions/")

  /** Source inputs. */
  override val sources: Seq[Input.Source] = Seq(regions)

  /** Only a single output.
    */
  override val rules: PartialFunction[Input, Outputs] = {
    case _ => Outputs.Named("regions")
  }

  /** Run the job for joining the tissue ontology with each region.
    */
  override def make(output: String): Job = {
    new Job(Job.PySpark(resourceUri("joinTissues.py")))
  }
}
