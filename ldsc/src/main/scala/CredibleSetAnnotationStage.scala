package org.broadinstitute.dig.aggregator.methods.ldsc

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws.emr._

class CredibleSetAnnotationStage(implicit context: Context) extends Stage {

  val regions: Input.Source = Input.Source.Success("out/ldsc/regions/merged/annotation-tissue-biosample/*/")
  val credibleSets: Input.Source = Input.Source.Dataset("credible_sets/*/*/")

  /** Source inputs. */
  override val sources: Seq[Input.Source] = Seq(regions, credibleSets)

  /** Just need a single machine with no applications, but a good drive. */
  override def cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    releaseLabel = ReleaseLabel("emr-6.7.0")
  )

  // A new pattern for trying to update only combinations that need updating (see below)
  var allRegions: Set[RegionOutput] = Set[RegionOutput]()
  var alteredRegions: Set[RegionOutput] = Set[RegionOutput]()
  var allCredibleSets: Set[CredibleSetOutput] = Set[CredibleSetOutput]()
  var alteredCredibleSets: Set[CredibleSetOutput] = Set[CredibleSetOutput]()

  override val rules: PartialFunction[Input, Outputs] = {
    case regions(region) =>
      val Seq(annotation, tissue, _) = region.split("___").toSeq
      val regionOutput = RegionOutput(annotation, tissue)
      allRegions ++= Set(regionOutput)
      Outputs.Named(regionOutput.toOutput)
    case credibleSets(dataset, phenotype) =>
      val credibleSetOutput = CredibleSetOutput(dataset, phenotype)
      allCredibleSets ++= Set(credibleSetOutput)
      Outputs.Named(credibleSetOutput.toOutput)
  }

  // The idea here is that if a region comes up it is added to the altered regions, and then runs against all credible sets
  // Then if a credible set (e.g.) comes up it'll run against all the regions minus the already altered regions
  // Etc. This should results in a minimal set of jobs being defined and run
  override def make(output: String): Job = {
    val jobs: Seq[Job.Step] = CredibleSetAnnotationOutput.fromOutput(output) match {
      case output: CredibleSetOutput =>
        alteredCredibleSets ++= Set(output)
        (allRegions -- alteredRegions).map { region =>
          Job.PySpark(
            resourceUri("credibleSetAnnotation.py"),
            s"--annotation=${region.annotation}",
            s"--tissue=${region.tissue}",
            s"--credible-set-path=${output.dataset}/${output.phenotype}"
          )
        }.toSeq
      case output: RegionOutput =>
        alteredRegions ++= Set(output)
        (allCredibleSets -- alteredCredibleSets).map { credibleSet =>
          Job.PySpark(
            resourceUri("credibleSetAnnotation.py"),
            s"--annotation=${output.annotation}",
            s"--tissue=${output.tissue}",
            s"--credible-set-path=${credibleSet.dataset}/${credibleSet.phenotype}"
          )
        }.toSeq
    }
    new Job(jobs, parallelSteps=true)
  }
}

trait CredibleSetAnnotationOutput

case class CredibleSetOutput(
  dataset: String,
  phenotype: String
) extends CredibleSetAnnotationOutput {
  def toOutput: String = s"credible_set/$dataset/$phenotype"
}

case class RegionOutput(
  annotation: String,
  tissue: String
) extends CredibleSetAnnotationOutput {
  def toOutput: String = s"region/$annotation/$tissue"
}

object CredibleSetAnnotationOutput {
  def fromOutput(output: String): CredibleSetAnnotationOutput = output.split("/").toSeq match {
    case Seq("credible_set", dataset, phenotype) => CredibleSetOutput(dataset, phenotype)
    case Seq("region", annotation, tissue) => RegionOutput(annotation, tissue)
    case _ => throw new Exception("Invalid String")
  }
}
