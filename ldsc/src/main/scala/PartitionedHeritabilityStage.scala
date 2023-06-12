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

  var allPhenotypeAncestries: Set[PartitionedHeritabilityPhenotype] = Set()
  lazy val phenotypeMap: Map[String, Set[PartitionedHeritabilityPhenotype]] = allPhenotypeAncestries.groupBy(_.ancestry)
  var allAnnotations: Set[PartitionedHeritabilityRegion] = Set()
  lazy val annotationMap: Map[String, Set[PartitionedHeritabilityRegion]] = allAnnotations.groupBy(_.subRegion)

  // TODO: At the moment this will always rerun everything which isn't ideal
  override val rules: PartialFunction[Input, Outputs] = {
    case sumstats(phenotype, ancestry) =>
      allPhenotypeAncestries ++= Set(PartitionedHeritabilityPhenotype(phenotype, ancestry.split('=').last))
      Outputs.Named("partitioned-heritability")
    case annotations(_, subRegion, region) =>
      allAnnotations ++= Set(PartitionedHeritabilityRegion(subRegion, region))
      Outputs.Named("partitioned-heritability")
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
    val jobs = phenotypeMap.flatMap { case (ancestry, phenotypes) =>
      phenotypes.grouped(100).flatMap { groupedPhenotypes =>
        annotationMap.flatMap { case (subRegion, regions) =>
          regions.grouped(100).map { groupedRegions =>
            Job.Script(
              resourceUri("runPartitionedHeritability.py",
                s"--ancestry=${ancestry}",
                s"--phenotypes=${groupedPhenotypes.mkString(",")}",
                s"--sub-region=$subRegion",
                s"--regions=${groupedRegions.mkString(",")}"
              )
            )
          }
        }
      }
    }.toSeq
    new Job(jobs, parallelSteps=true)
  }
}

case class PartitionedHeritabilityRegion(
  subRegion: String,
  region: String
)

case class PartitionedHeritabilityPhenotype(
  phenotype: String,
  ancestry: String
)
