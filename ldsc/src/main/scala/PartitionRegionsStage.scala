package org.broadinstitute.dig.aggregator.methods.ldsc

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws.emr._

class PartitionRegionsStage(implicit context: Context) extends Stage {
  val cisReg: Input.Source = Input.Source.Dataset("annotated_regions/cis-regulatory_elements/*/")

  /** Source inputs. */
  override val sources: Seq[Input.Source] = Seq(cisReg)

  /** Map inputs to their outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case cisReg(dataset) => Outputs.Named(dataset)
   }

  // Partitioning is quick and easy, so do many of them in parallel.
  override def cluster: ClusterDef = super.cluster.copy(
    stepConcurrency = 5
  )

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
