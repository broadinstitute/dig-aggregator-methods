package org.broadinstitute.dig.aggregator.methods.ldsc

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy

class AnnotToLDStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val annotFiles: Input.Source = Input.Source.Success(s"out/ldsc/regions/annot/*/*/*/")

  /** Source inputs. */
  override val sources: Seq[Input.Source] = Seq(annotFiles)

  /** Just need a single machine with no applications, but a good drive. */
  override def cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    applications = Seq.empty,
    masterInstanceType = Strategy.generalPurpose(vCPUs = 16),
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("install-ldscore.sh"))),
    releaseLabel = ReleaseLabel("emr-6.7.0")
  )

  var regionCache: Seq[AnnotToLDRegion] = Seq.empty
  lazy val regionMap: Map[(String, String), Seq[AnnotToLDRegion]] = regionCache.groupBy(output => output.group.key)

  override val rules: PartialFunction[Input, Outputs] = {
    case annotFiles(ancestry, subRegion, region) =>
      val annotToLDRegion = AnnotToLDRegion(ancestry.split("=").last, subRegion, region)
      regionCache ++= Seq(annotToLDRegion)
      Outputs.Named(annotToLDRegion.group.toOutput)
  }

  /** The partition names are combined together across datasets into single
    * BED files that can then be read by GREGOR.
    */
  override def make(output: String): Job = {
    val group = AnnotToLDGroup.fromOutput(output)
    val jobs = regionMap.getOrElse(group.key, Seq()).grouped(100).map { subGroup =>
      println(s"Adding group for ${group.ancestry} and ${group.subRegion} with ${subGroup.length} regions")
      Job.Script(
        resourceUri("annotToLD.py"),
        s"--ancestry=${group.ancestry}",
        s"--sub-region=${group.subRegion}",
        s"--regions=${subGroup.map(_.region).mkString(",")}"
      )
    }.toSeq
    new Job(jobs)
  }

  /** Before the jobs actually run, perform this operation.
    */
  override def prepareJob(output: String): Unit = {
    regionCache.map { region =>
      context.s3.rm(s"${region.directory}/")
    }
  }

  /** Update the success flag of the merged regions.
    */
  override def success(output: String): Unit = {
    regionCache.map { region =>
      context.s3.rm(s"${region.directory}/_SUCCESS")
    }
    ()
  }
}

case class AnnotToLDRegion(
  ancestry: String,
  subRegion: String,
  region: String
) {
  def directory: String = s"out/ldsc/regions/ld_score/ancestry=$ancestry/$subRegion/$region"
  val group: AnnotToLDGroup = AnnotToLDGroup(ancestry, subRegion)
}

case class AnnotToLDGroup(
  ancestry: String,
  subRegion: String
) {
  def key: (String, String) = (ancestry, subRegion)
  def toOutput: String = s"$ancestry/$subRegion"
}

object AnnotToLDGroup {
  def fromOutput(output: String): AnnotToLDGroup = output.split("/").toSeq match {
    case Seq(ancestry, subRegion) => AnnotToLDGroup(ancestry, subRegion)
  }
}
