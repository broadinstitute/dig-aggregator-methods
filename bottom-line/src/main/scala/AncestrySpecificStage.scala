package org.broadinstitute.dig.aggregator.methods.bottomline

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy

class AncestrySpecificStage(implicit context: Context) extends Stage {
  import Implicits.S3Key

  val variants: Input.Source = Input.Source.Success("out/metaanalysis/variants/*/*/")

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    applications = Seq.empty,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("cluster-bootstrap.sh")))
  )

  override val sources: Seq[Input.Source] = Seq(variants)

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
    val ancestrySpecific = resourceUri("runAncestrySpecific.sh")
    val ancestrySpecificOutput = AncestrySpecificOutput.fromOutput(output)

    new Job(Seq(Job.Script(ancestrySpecific, ancestrySpecificOutput.phenotype, ancestrySpecificOutput.ancestry)))
  }
}

case class S3Partition(
  phenotype: String,
  dataset: String,
  ancestry: String
) {
  def toAncestrySpecificOutput: AncestrySpecificOutput = AncestrySpecificOutput(phenotype, ancestry)
}

case class AncestrySpecificOutput(
  phenotype: String,
  ancestry: String
) {
  def toOutput: String = s"$phenotype/$ancestry"
}

object AncestrySpecificOutput {
  def fromOutput(output: String): AncestrySpecificOutput = {
    output.split("/").toSeq match {
      case Seq(phenotype, ancestry) => AncestrySpecificOutput(phenotype, ancestry)
      case _ => throw new Exception("Invalid AncestrySpecificOutput output string")
    }
  }
}
