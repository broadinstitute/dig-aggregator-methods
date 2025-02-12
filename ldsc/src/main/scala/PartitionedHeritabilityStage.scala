package org.broadinstitute.dig.aggregator.methods.ldsc

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy
import org.broadinstitute.dig.aws.MemorySize

class PartitionedHeritabilityStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val sumstats: Input.Source = Input.Source.Raw("out/ldsc/sumstats/*/*/*.sumstats.gz")
  val portalBucket: S3.Bucket = new S3.Bucket("dig-analysis-data", None)
  val portalAnnotations: Input.Source = Input.Source.Success(
    s"out/ldsc/regions/combined_ld/*/*/*/",
    s3BucketOverride = Some(portalBucket)
  )
  val projectAnnotations: Input.Source = Input.Source.Success(s"out/ldsc/regions/combined_ld/*/*/*/")

  /** Source inputs. */
  override val sources: Seq[Input.Source] = Seq(sumstats, portalAnnotations, projectAnnotations)

  var allPhenotypeAncestries: Set[PartitionedHeritabilityPhenotype] = Set()
  lazy val phenotypeMap: Map[String, Set[String]] = allPhenotypeAncestries.groupBy(_.ancestry).map {
    case (ancestry, phenotypes) => ancestry -> phenotypes.map(_.phenotype)
  }
  var allProjectAnnotations: Set[PartitionedHeritabilityRegion] = Set()
  lazy val allProjectAnnotationMap: Map[String, Set[String]] = allProjectAnnotations.groupBy(_.subRegion).map {
    case (subRegion, regions) => subRegion -> regions.map(_.region)
  }
  var allPortalAnnotations: Set[PartitionedHeritabilityRegion] = Set()
  lazy val allPortalAnnotationMap: Map[String, Set[String]] = allPortalAnnotations.groupBy(_.subRegion).map {
    case (subRegion, regions) => subRegion -> regions.map(_.region)
  }
  lazy val annotationMap: Map[String, Map[String, Set[String]]] = Map(
    //"portal" -> allPortalAnnotationMap,
    context.project -> allProjectAnnotationMap
  )

  // TODO: At the moment this will always rerun everything which isn't ideal
  override val rules: PartialFunction[Input, Outputs] = {
    case sumstats(phenotype, ancestry, _) =>
      allPhenotypeAncestries ++= Set(PartitionedHeritabilityPhenotype(phenotype, ancestry.split('=').last))
      Outputs.Named(ancestry.split('=').last)
    case projectAnnotations(_, subRegion, region) => if (context.project != "portal") {
      allProjectAnnotations ++= Set(PartitionedHeritabilityRegion(subRegion, region))
      Outputs.All
    } else Outputs.Null
    case portalAnnotations(_, subRegion, region) =>
      allPortalAnnotations ++= Set(PartitionedHeritabilityRegion(subRegion, region))
      Outputs.All
  }

  /** Just need a single machine with no applications, but a good drive. */
  override def cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    applications = Seq.empty,
    masterVolumeSizeInGB = 100,
    bootstrapScripts = Seq(
      new BootstrapScript(resourceUri("install-ldscore.sh")),
      new BootstrapScript(
        resourceUri("downloadAnnotFiles.py"), s"--input-path=s3://${context.s3.path}", s"--project=${context.project}"
      )
    ),
    masterInstanceType = Strategy.generalPurpose(vCPUs = 16)
  )

  override def make(ancestry: String): Job = {
    val jobs = phenotypeMap.getOrElse(ancestry, Set()).grouped(100).flatMap { groupedPhenotypes =>
      annotationMap.flatMap { case (project, projectRegions) =>
        projectRegions.flatMap { case (subRegion, regions) =>
          regions.grouped(40).map { groupedRegion =>
            println(s"creating Job for ${groupedPhenotypes.size} phenotypes in ancestry $ancestry " +
              s"and region ${groupedRegion.size} in sub-region $subRegion")
            Job.Script(
              resourceUri("runPartitionedHeritability.py"),
              s"--ancestry=${ancestry}",
              s"--phenotypes=${groupedPhenotypes.mkString(",")}",
              s"--sub-region=$subRegion",
              s"--region=${groupedRegion.mkString(",")}",
              s"--project=$project"
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
