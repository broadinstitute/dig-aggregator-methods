package org.broadinstitute.dig.aggregator.methods.bioindex

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

/** The final result of all aggregator methods is building the BioIndex. All
  * outputs are to the dig-bio-index bucket in S3.
  */
class AnnotatedRegionsStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val partitions: Seq[String] = Seq()
  val subRegion: String = if (partitions.isEmpty) "gregor_backup" else partitions.mkString("-")
  val regions = Input.Source.Success(s"out/ldsc/regions/$subRegion/merged/")

  /** Use memory-optimized machine with sizeable disk space for shuffling. */
  override val cluster: ClusterDef = super.cluster.copy(
    masterInstanceType = Ec2.Strategy.memoryOptimized(mem = 128.gb),
    slaveInstanceType = Ec2.Strategy.memoryOptimized(mem = 128.gb),
    instances = 5
  )

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(regions)

  /** Rules for mapping input to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case _ => Outputs.Named("annotations")
  }

  /** Output to Job steps. */
  override def make(output: String): Job = {
    new Job(Job.PySpark(resourceUri("annotatedRegions.py"), subRegion))
  }
}
