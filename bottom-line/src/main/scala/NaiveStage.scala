package org.broadinstitute.dig.aggregator.methods.bottomline

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy

class NaiveStage(implicit context: Context) extends Stage {
  import Implicits.S3Key

  val variants: Input.Source = Input.Source.Success("out/metaanalysis/variants/*/*/")

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 6,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("naive-bootstrap.sh"))),
    releaseLabel = ReleaseLabel("emr-6.7.0") // Need emr 6.1+ to read zstd files
  )

  override val sources: Seq[Input.Source] = Seq(variants)

  // Makes only one call to get all of the files. Lazy as this class is instantiated whenever any stage it called
  lazy val naivePartitionMap: Map[(String, String), Seq[NaivePartition]] = {
    val part = "/([^/]+)/dataset=([^/]+)/ancestry=([^/]+)/".r
    val parts: Seq[NaivePartition] = context.s3
      .ls(s"${variants.prefix.commonPrefix}").flatMap { obj =>
      part.findFirstMatchIn(obj.key)
        .map { naiveMatch =>
          NaivePartition(naiveMatch.group(1), naiveMatch.group(2), naiveMatch.group(3))
        }
    }.distinct
    // Create map (phenotype, dataset) -> NaivePartition
    parts.groupBy(naiveLocation => (naiveLocation.phenotype, naiveLocation.dataset)).toMap
  }

  override val rules: PartialFunction[Input, Outputs] = {
    case variants(phenotype: String, dataset: String) =>
      val partitions: Seq[NaivePartition] = naivePartitionMap
        .get((phenotype, dataset.split("=").last))
        .toSeq
        .flatten
      // if there are no partitions, then we can ignore it
      if (partitions.nonEmpty) {
        val partitionNames = partitions.map(_.toNaiveOutput.toOutput)
        Outputs.Named(partitionNames: _*)
      } else Outputs.Null
  }

  override def make(output: String): Job = {
    val naive = resourceUri("runNaive.py")
    val naiveOutput = NaiveOutput.fromOutput(output)

    new Job(Seq(Job.PySpark(naive, naiveOutput.phenotype, naiveOutput.ancestry)))
  }
}

case class NaivePartition(
  phenotype: String,
  dataset: String,
  ancestry: String
) {
  def toNaiveOutput: NaiveOutput = NaiveOutput(phenotype, ancestry)
}

case class NaiveOutput(
  phenotype: String,
  ancestry: String
) {
  def toOutput: String = s"$phenotype/$ancestry"
}

object NaiveOutput {
  def fromOutput(output: String): NaiveOutput = {
    output.split("/").toSeq match {
      case Seq(phenotype, ancestry) => NaiveOutput(phenotype, ancestry)
      case _ => throw new Exception("Invalid NaiveOutput output string")
    }
  }
}
