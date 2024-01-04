package org.broadinstitute.dig.aggregator.methods.ldsc

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy
import org.broadinstitute.dig.aws.MemorySize

class PartitionedHeritabilityStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val sumstats: Input.Source = Input.Source.Success("out/ldsc/sumstats/*/*/")
  val annotations: Input.Source = Input.Source.Success(s"out/ldsc/regions/combined_ld/*/*/*/")

  val annotationMap: Map[String, Set[String]] = Map(
    "annotation-tissue-biosample" -> Set(
      "accessible_chromatin___blood",
      "accessible_chromatin___central_nervous_system",
      "accessible_chromatin___large_intestine",
      "accessible_chromatin___muscle_structure",
      "accessible_chromatin___simple_tissue",
      "accessible_chromatin___skin_of_body",
      "binding_sites___blood",
      "binding_sites___eye",
      "binding_sites___heart",
      "binding_sites___large_intestine",
      "binding_sites___lung",
      "binding_sites___mammary_gland",
      "enhancer___blood",
      "enhancer___blood_vessel",
      "enhancer___cardiovascular_system",
      "enhancer___mammary_gland",
      "enhancer___placenta",
      "enhancer___uterus",
      "promoter___blood",
      "promoter___central_nervous_system",
      "promoter___esophagus",
      "promoter___eye",
      "promoter___gastrointestinal_system",
      "promoter___simple_tissue",
      "promoter___skin_of_body"
    )
  )

  /** Source inputs. */
  override val sources: Seq[Input.Source] = Seq(sumstats, annotations)

  var allPhenotypeAncestries: Set[PartitionedHeritabilityPhenotype] = Set()
  lazy val phenotypeMap: Map[String, Set[String]] = allPhenotypeAncestries.groupBy(_.ancestry).map {
    case (ancestry, phenotypes) => ancestry -> phenotypes.map(_.phenotype)
  }
//  var allAnnotations: Set[PartitionedHeritabilityRegion] = Set()
//  lazy val annotationMap: Map[String, Set[String]] = allAnnotations.groupBy(_.subRegion).map {
//    case (subRegion, regions) => subRegion -> regions.map(_.region)
//  }

  // TODO: At the moment this will always rerun everything which isn't ideal
  override val rules: PartialFunction[Input, Outputs] = {
    case sumstats(phenotype, ancestry) =>
      allPhenotypeAncestries ++= Set(PartitionedHeritabilityPhenotype(phenotype, ancestry.split('=').last))
      Outputs.Named(ancestry.split('=').last)
    case annotations(_, subRegion, region) =>
      Outputs.Null
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

  override def make(ancestry: String): Job = {
    val jobs = phenotypeMap.getOrElse(ancestry, Set()).grouped(100).flatMap { groupedPhenotypes =>
      annotationMap.flatMap { case (subRegion, regions) =>
        regions.map { region =>
          println(s"creating Job for ${groupedPhenotypes.size} phenotypes in ancestry $ancestry " +
            s"and region $region in sub-region $subRegion")
          Job.Script(
            resourceUri("runPartitionedHeritability.py"),
            s"--ancestry=${ancestry}",
            s"--phenotypes=${groupedPhenotypes.mkString(",")}",
            s"--sub-region=$subRegion",
            s"--region=$region"
          )
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
