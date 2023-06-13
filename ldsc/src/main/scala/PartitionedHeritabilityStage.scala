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
  lazy val phenotypeMap: Map[String, Set[String]] = allPhenotypeAncestries.groupBy(_.ancestry).map {
    case (ancestry, phenotypes) => ancestry -> phenotypes.map(_.phenotype)
  }
  var allAnnotations: Set[PartitionedHeritabilityRegion] = Set()
  lazy val annotationMap: Map[String, Set[String]] = allAnnotations.groupBy(_.subRegion).map {
    case (subRegion, regions) => subRegion -> regions.map(_.region)
  }

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
    val jobs = phenotypeMap.filter(_._1 == "EU").flatMap { case (ancestry, phenotypes) =>
      phenotypes.filter(p => Seq("T2D", "BMI", "TG", "AF").contains(p)).grouped(100).flatMap { groupedPhenotypes =>
        annotationMap.filter(_._1 == "annotation-tissue").flatMap { case (subRegion, regions) =>
          regions.grouped(100).map { groupedRegions =>
            println(s"creating Job for ${groupedPhenotypes.size} phenotypes in ancestry $ancestry " +
              s"and ${groupedRegions.size} regions in sub-region $subRegion")
            Job.Script(
              resourceUri("runPartitionedHeritability.py"),
                s"--ancestry=${ancestry}",
                s"--phenotypes=${groupedPhenotypes.mkString(",")}",
                s"--sub-region=$subRegion",
                s"--regions=${groupedRegions.mkString(",")}"
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
