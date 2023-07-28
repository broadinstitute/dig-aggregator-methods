package org.broadinstitute.dig.aggregator.methods.bottomline

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy

class LoadAncestrySpecificStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._
  import org.broadinstitute.dig.aggregator.core.Implicits.S3Key

  val ancestrySpecificTables: Input.Source = Input.Source.Success("furkan-v1-new-2/out/metaanalysis/staging/ancestry-specific/*/*/")
  val variants: Input.Source = Input.Source.Success("furkan-v1-new-2/out/metaanalysis/variants/*/*/")

  // NOTE: Fairly slow, but this can run with a single m5.2xlarge if necessary
  // A cluster of 3 compute instances though makes it quite swift
  override val cluster: ClusterDef = super.cluster.copy(
    masterInstanceType = Strategy.computeOptimized(vCPUs = 8, mem = 16.gb),
    slaveInstanceType = Strategy.computeOptimized(vCPUs = 8, mem = 16.gb),
    instances = 4,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("cluster-bootstrap.sh"))),
    releaseLabel = ReleaseLabel("emr-6.7.0") // Need emr 6.1+ to read zstd files
  )

  /** A little overwrought, but this is done because
   * - Just checking the variants could allow someone to run the system out of order and not update the system properly
   * - But Just checking the tables means if, somehow, a dataset of only rare variants to not be processed properly
   * TODO: Fix the structure of the variants directory to allow for ancestries to be detected which would fix this
   * */
  override val sources: Seq[Input.Source] = Seq(ancestrySpecificTables, variants)

  // Makes only one call to get all of the files. Lazy as this class is instantiated whenever any stage it called
  lazy val s3PartitionMap: Map[(String, String), Seq[S3Partition]] = {
    val part = "/([^/]+)/dataset=([^/]+)/ancestry=([^/]+)/".r
    val parts: Seq[S3Partition] = context.s3
      .ls(s"${variants.prefix.commonPrefix}").flatMap { obj =>
      part.findFirstMatchIn(obj.key)
        .map { s3Match =>
          S3Partition(s3Match.group(1), s3Match.group(2), s3Match.group(3))
        }
    }.distinct
    // Create map (phenotype, dataset) -> S3Partition
    parts.groupBy(s3Location => (s3Location.phenotype, s3Location.dataset)).toMap
  }

  override val rules: PartialFunction[Input, Outputs] = {
    case ancestrySpecificTables(phenotype, ancestry) =>
      Outputs.Named(LoadAncestryOutput(phenotype, ancestry.split("=").last).toOutput)
    case variants(phenotype: String, dataset: String) =>
      // Filter out Mixed datasets as they don't go through ancestry specific path
      val partitions: Seq[S3Partition] = s3PartitionMap
        .get((phenotype, dataset.split("=").last))
        .toSeq
        .flatten
        .filter(_.ancestry != "Mixed")
      // if there are no partitions, then we can ignore it
      if (partitions.nonEmpty) {
        val partitionNames = partitions.map(_.toAncestrySpecificOutput.toOutput)
        Outputs.Named(partitionNames: _*)
      } else Outputs.Null
  }

  override def make(output: String): Job = {
    val loadAnalysis = resourceUri("loadAnalysis.py")
    val loadAncestryOutput = LoadAncestryOutput.fromOutput(output)

    new Job(Seq(Job.PySpark(
      loadAnalysis,
      "--ancestry-specific",
      "--phenotype", loadAncestryOutput.phenotype,
      "--ancestry", loadAncestryOutput.ancestry
    )))
  }
}

case class LoadAncestryOutput(
 phenotype: String,
 ancestry: String
) {
  def toOutput: String = s"$phenotype/$ancestry"
}

object LoadAncestryOutput {
  def fromOutput(output: String): LoadAncestryOutput = {
    output.split("/").toSeq match {
      case Seq(phenotype, ancestry) => LoadAncestryOutput(phenotype, ancestry)
      case _ => throw new Exception("Invalid LoadAncestryOutput output string")
    }
  }
}
