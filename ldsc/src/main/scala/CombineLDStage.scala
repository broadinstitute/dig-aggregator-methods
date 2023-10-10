package org.broadinstitute.dig.aggregator.methods.ldsc

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy

class CombineLDStage(implicit context: Context) extends Stage {

  val ldFiles: Input.Source = Input.Source.Success(s"out/ldsc/regions/ld_score/*/*/*/")

  /** Source inputs. */
  override val sources: Seq[Input.Source] = Seq(ldFiles)

  /** Just need a single machine with no applications, but a good drive. */
  override def cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    applications = Seq.empty,
    releaseLabel = ReleaseLabel("emr-6.7.0"),
    stepConcurrency = 5
  )

  override val rules: PartialFunction[Input, Outputs] = {
    case ldFiles(ancestry, subRegion, region) if subRegion == "annotation-tissue" =>
      Outputs.Named(CombineLDTissue(ancestry.split("=").last, region).toOutput)
    case ldFiles(ancestry, subRegion, region) if subRegion == "annotation-tissue-biosample" =>
      Outputs.Named(CombineLDBiosample(ancestry.split("=").last, region).toOutput)
    case _ => Outputs.Null
  }

  override def make(output: String): Job = {
    val combineLD = CombineLD.fromOutput(output)
    new Job(Job.Script(
      resourceUri("combineLD.py"),
      s"--ancestry=${combineLD.ancestry}",
      s"--annotation=${combineLD.annotation}",
      s"--tissue=${combineLD.tissue}"
    ))
  }
}

case class CombineLDTissue(
  ancestry: String,
  region: String
) {
  val Seq(annotation, tissue) = region.split("___").toSeq
  val toOutput: String = s"$ancestry/$annotation/$tissue"
}

case class CombineLDBiosample(
  ancestry: String,
  region: String
) {
  val Seq(annotation, tissue, _) = region.split("___").toSeq
  val toOutput: String = s"$ancestry/$annotation/$tissue"
}

case class CombineLD(
  ancestry: String,
  annotation: String,
  tissue: String
)

object CombineLD {
  def fromOutput(output: String): CombineLD = output.split("/").toSeq match {
    case Seq(ancestry, annotation, tissue) => CombineLD(ancestry, annotation, tissue)
  }
}
