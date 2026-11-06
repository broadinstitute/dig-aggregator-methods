package org.broadinstitute.dig.aggregator.methods.ldsc

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws.emr._

class PartitionRegionsStage(implicit context: Context) extends Stage {
  val cisReg: Input.Source = Input.Source.Dataset("annotated_regions/cis-regulatory_elements/*/*/")

  /** Source inputs. */
  override val sources: Seq[Input.Source] = Seq(cisReg)

  /** Map inputs to their outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case cisReg(project, dataset) => Outputs.Named(s"$project/$dataset")
   }

  // Partitioning is quick and easy, so do many of them in parallel.
  override def cluster: ClusterDef = super.cluster.copy(
    stepConcurrency = 5
  )

  override def make(output: String): Job = {
    val flags: Seq[String] = output.split("/").toSeq match {
      case Seq(project, dataset) => Seq(s"--project=$project", s"--dataset=$dataset")
    }

    new Job(Job.PySpark(resourceUri("partitionRegions.py"), flags:_*))
  }
}
