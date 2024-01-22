package org.broadinstitute.dig.aggregator.methods.ldsc

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws.emr._

class RegionToAnnotStage(implicit context: Context) extends Stage {

  val ancestries: Seq[String] = Seq("AFR", "AMR", "EAS", "EUR", "SAS")
  val mergedFiles: Input.Source = Input.Source.Success(s"out/ldsc/regions/merged/*/*/")

  /** Source inputs. */
  override val sources: Seq[Input.Source] = Seq(mergedFiles)

  /** Just need a single machine with no applications, but a good drive. */
  override def cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    applications = Seq.empty,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("install-ldscore.sh")))
    stepConcurrency = 7
  )

  override val rules: PartialFunction[Input, Outputs] = {
    case mergedFiles(subRegion, region) => Outputs.Named(RegionToAnnotOutput(subRegion, region).toOutput)
  }

  override def make(output: String): Job = {
    val regionToAnnotOutput: RegionToAnnotOutput = RegionToAnnotOutput.fromOutput(output)
    new Job(Job.Script(
      resourceUri("regionsToAnnot.py"),
      s"--sub-region=${regionToAnnotOutput.subRegion}",
      s"--region-name=${regionToAnnotOutput.region}",
      s"--ancestries=${ancestries.mkString(",")}"
    ))
  }

  /** Update the success flag of the merged regions.
    */
  override def success(output: String): Unit = {
    val regionToAnnotOutput: RegionToAnnotOutput = RegionToAnnotOutput.fromOutput(output)
    ancestries.foreach { ancestry =>
      context.s3.touch(s"out/ldsc/regions/annot/ancestry=$ancestry/" +
        s"${regionToAnnotOutput.subRegion}/" +
        s"${regionToAnnotOutput.region}/_SUCCESS")
    }
    ()
  }
}

case class RegionToAnnotOutput(
  subRegion: String,
  region: String
) {
  def toOutput: String = s"$subRegion/$region"
}

object RegionToAnnotOutput {
  def fromOutput(output: String): RegionToAnnotOutput = output.split("/").toSeq match {
    case Seq(subRegion, region) => RegionToAnnotOutput(subRegion, region)
    case _ => throw new Exception("Invalid String")
  }
}
