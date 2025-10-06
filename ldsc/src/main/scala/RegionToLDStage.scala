package org.broadinstitute.dig.aggregator.methods.ldsc

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy

class RegionToLDStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val ancestries: Seq[String] = Seq("AFR", "AMR", "EAS", "EUR", "SAS")
  val mergedFiles: Input.Source = Input.Source.Success(s"out/ldsc/regions/merged/*/*/")

  /** Source inputs. */
  override val sources: Seq[Input.Source] = Seq(mergedFiles)

  /** Just need a single machine with no applications, but a good drive. */
  override def cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    applications = Seq.empty,
    masterInstanceType = Strategy.generalPurpose(vCPUs = 1),
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("install-ldscore.sh")))
  )

  override val rules: PartialFunction[Input, Outputs] = {
    case mergedFiles(subRegion, region) => Outputs.Named(RegionToLDOutput(subRegion, region).toOutput)
  }

  override def make(output: String): Job = {
    val regionToLDOutput: RegionToLDOutput = RegionToLDOutput.fromOutput(output)
    new Job(Job.Script(
      resourceUri("regionsToLD.py"),
      s"--sub-region=${regionToLDOutput.subRegion}",
      s"--region-name=${regionToLDOutput.region}",
      s"--ancestries=${ancestries.mkString(",")}"
    ))
  }
}

case class RegionToLDOutput(
  subRegion: String,
  region: String
) {
  def toOutput: String = s"$subRegion/$region"
}

object RegionToLDOutput {
  def fromOutput(output: String): RegionToLDOutput = output.split("/").toSeq match {
    case Seq(subRegion, region) => RegionToLDOutput(subRegion, region)
    case _ => throw new Exception("Invalid String")
  }
}
