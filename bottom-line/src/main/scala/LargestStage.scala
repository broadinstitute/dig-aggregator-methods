package org.broadinstitute.dig.aggregator.methods.bottomline

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy

class LargestStage(implicit context: Context) extends Stage {
  import Implicits.S3Key

  val variants: Input.Source = Input.Source.Success("out/metaanalysis/variants/*/*/")

  override val cluster: ClusterDef = super.cluster.copy(
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("largest-bootstrap.sh")))
  )

  override val sources: Seq[Input.Source] = Seq(variants)

  // Makes only one call to get all of the files. Lazy as this class is instantiated whenever any stage it called
  lazy val largestPartitionMap: Map[(String, String), Seq[LargestPartition]] = {
    val part = "/([^/]+)/dataset=([^/]+)/ancestry=([^/]+)/".r
    val parts: Seq[LargestPartition] = context.s3
      .ls(s"${variants.prefix.commonPrefix}").flatMap { obj =>
      part.findFirstMatchIn(obj.key)
        .map { largestMatch =>
          LargestPartition(largestMatch.group(1), largestMatch.group(2), largestMatch.group(3))
        }
    }.distinct
    // Create map (phenotype, dataset) -> LargestPartition
    parts.groupBy(largestLocation => (largestLocation.phenotype, largestLocation.dataset)).toMap
  }

  override val rules: PartialFunction[Input, Outputs] = {
    case variants(phenotype: String, dataset: String) =>
      val partitions: Seq[LargestPartition] = largestPartitionMap
        .get((phenotype, dataset.split("=").last))
        .toSeq
        .flatten
      // if there are no partitions, then we can ignore it
      if (partitions.nonEmpty) {
        val partitionNames = partitions.flatMap(_.toLargestOutputs.map(_.toOutput))
        Outputs.Named(partitionNames: _*)
      } else Outputs.Null
  }

  override def make(output: String): Job = {
    val largest = resourceUri("runLargest.py")
    val largestOutput = LargestOutput.fromOutput(output)

    new Job(Seq(Job.PySpark(largest, largestOutput.phenotype, largestOutput.ancestry)))
  }
}

case class LargestPartition(
  phenotype: String,
  dataset: String,
  ancestry: String
) {
  def toLargestOutputs: Seq[LargestOutput] = Seq(LargestOutput(phenotype, ancestry), LargestOutput(phenotype, "TE"))
}

case class LargestOutput(
 phenotype: String,
 ancestry: String
) {
  def toOutput: String = s"$phenotype/$ancestry"
}

object LargestOutput {
  def fromOutput(output: String): LargestOutput = {
    output.split("/").toSeq match {
      case Seq(phenotype, ancestry) => LargestOutput(phenotype, ancestry)
      case _ => throw new Exception("Invalid LargestOutput output string")
    }
  }
}
