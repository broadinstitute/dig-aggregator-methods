package org.broadinstitute.dig.aggregator.methods.bottomline

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy

class MinPStage(implicit context: Context) extends Stage {
  import Implicits.S3Key

  val variants: Input.Source = Input.Source.Success("out/metaanalysis/variants/*/*/")

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 6,
    releaseLabel = ReleaseLabel("emr-6.7.0") // Need emr 6.1+ to read zstd files
  )

  override val sources: Seq[Input.Source] = Seq(variants)

  // Makes only one call to get all of the files. Lazy as this class is instantiated whenever any stage it called
  lazy val minPPartitionMap: Map[(String, String), Seq[MinPPartition]] = {
    val part = "/([^/]+)/dataset=([^/]+)/ancestry=([^/]+)/".r
    val parts: Seq[MinPPartition] = context.s3
      .ls(s"${variants.prefix.commonPrefix}").flatMap { obj =>
      part.findFirstMatchIn(obj.key)
        .map { minPMatch =>
          MinPPartition(minPMatch.group(1), minPMatch.group(2), minPMatch.group(3))
        }
    }.distinct
    // Create map (phenotype, dataset) -> MinPPartition
    parts.groupBy(minPLocation => (minPLocation.phenotype, minPLocation.dataset)).toMap
  }

  override val rules: PartialFunction[Input, Outputs] = {
    case variants(phenotype: String, dataset: String) =>
      val partitions: Seq[MinPPartition] = minPPartitionMap
        .get((phenotype, dataset.split("=").last))
        .toSeq
        .flatten
      // if there are no partitions, then we can ignore it
      if (partitions.nonEmpty) {
        val partitionNames = partitions.map(_.toMinPOutput.toOutput)
        Outputs.Named(partitionNames: _*)
      } else Outputs.Null
  }

  override def make(output: String): Job = {
    val minP = resourceUri("runMinP.py")
    val minPOutput = MinPOutput.fromOutput(output)

    new Job(Seq(Job.PySpark(minP, minPOutput.phenotype, minPOutput.ancestry)))
  }
}

case class MinPPartition(
  phenotype: String,
  dataset: String,
  ancestry: String
) {
  def toMinPOutput: MinPOutput = MinPOutput(phenotype, ancestry)
}

case class MinPOutput(
  phenotype: String,
  ancestry: String
) {
  def toOutput: String = s"$phenotype/$ancestry"
}

object MinPOutput {
  def fromOutput(output: String): MinPOutput = {
    output.split("/").toSeq match {
      case Seq(phenotype, ancestry) => MinPOutput(phenotype, ancestry)
      case _ => throw new Exception("Invalid MinPOutput output string")
    }
  }
}
