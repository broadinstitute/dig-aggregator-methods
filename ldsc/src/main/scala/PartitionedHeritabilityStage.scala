package org.broadinstitute.dig.aggregator.methods.ldsc

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy
import org.broadinstitute.dig.aws.MemorySize

class PartitionedHeritabilityStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val sumstats: Input.Source = Input.Source.Success("out/ldsc/sumstats/*/*/")
  val annotations: Input.Source = Input.Source.Success(s"out/ldsc/regions/ld_score/*/*/*/")

  /** Source inputs. */
  override val sources: Seq[Input.Source] = Seq(sumstats, annotations)

  // TODO: Will always be all annotations. Can this also record modified annotations?
  var allAnnotations: Set[PartitionedHeritabilityRegion] = Set()
  lazy val annotationMap: Map[String, Set[PartitionedHeritabilityRegion]] = allAnnotations.groupBy(_.subRegion)

  /** Map inputs to their outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case sumstats(phenotype, ancestry) =>
      Outputs.Named(PartitionedHeritabilityInput(phenotype, ancestry.split('=').last).toOutput)
    case annotations(_, subRegion, region) =>
      allAnnotations ++= Set(PartitionedHeritabilityRegion(subRegion, region))
      Outputs.Null  // All annotations are run together
  }

  /** Just need a single machine with no applications, but a good drive. */
  override def cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    applications = Seq.empty,
    bootstrapScripts = Seq(
      new BootstrapScript(resourceUri("install-ldscore.sh"))
    ),
    masterInstanceType = Strategy.generalPurpose(vCPUs = 8),
    releaseLabel = ReleaseLabel("emr-6.7.0")
  )

  override def make(output: String): Job = {
    val input = PartitionedHeritabilityInput.fromString(output)
    val jobs = annotationMap.flatMap { case(subRegion, regions) =>
      regions.grouped(100).map { groupedRegions =>
        Job.Script(
          resourceUri("runPartitionedHeritability.py"),
          s"--phenotype=${input.phenotype}",
          s"--ancestry=${input.ancestry}",
          s"--subRegion=$subRegion",
          s"--phenotype=${groupedRegions.mkString(",")}"
        )
      }
    }.toSeq
    new Job(jobs, parallelSteps=true)
  }

}

case class PartitionedHeritabilityInput(
  phenotype: String,
  ancestry: String
) {
  def toOutput: String = s"$phenotype/$ancestry"
}

object PartitionedHeritabilityInput {
  def fromString(output: String): PartitionedHeritabilityInput = {
    output.split("/").toSeq match {
      case Seq(phenotype, ancestry) => PartitionedHeritabilityInput(phenotype, ancestry)
    }
  }
}

case class PartitionedHeritabilityRegion(
  subRegion: String,
  region: String
)
