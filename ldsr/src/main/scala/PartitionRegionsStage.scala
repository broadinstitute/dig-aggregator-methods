package org.broadinstitute.dig.aggregator.methods.ldsr

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws.emr.Job

class PartitionRegionsStage(implicit context: Context) extends Stage {
  val annotatedRegions: Input.Source = Input.Source.Dataset("annotated_regions/*/")

  /** Source inputs. */
  override val sources: Seq[Input.Source] = Seq(annotatedRegions)

  /** Map inputs to their outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case annotatedRegions(dataset) => Outputs.Named(dataset)
  }

  /** Take any new datasets and convert them from JSON-list to BED file
    * format with all the appropriate headers and fields. All the datasets
    * are processed together by the Spark job, so what's in the results
    * input doesn't matter.
    */
  override def make(output: String): Job = {
    val script  = resourceUri("partitionRegions.py")
    val dataset = output

    new Job(Job.PySpark(script, dataset))
  }
}
