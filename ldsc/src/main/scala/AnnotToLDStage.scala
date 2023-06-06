package org.broadinstitute.dig.aggregator.methods.ldsc

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy

class AnnotToLDStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val partitions: Seq[String] = Seq()
  val subRegion: String = if (partitions.isEmpty) "default" else partitions.mkString("-")
  val annotFiles: Input.Source = Input.Source.Success(s"out/ldsc/regions/${subRegion}/annot/*/*/")

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

  override val rules: PartialFunction[Input, Outputs] = {
    case annotFiles(ancestry, _) => Outputs.Named(ancestry.split("=").last)
  }

  override def make(output: String): Job = {
    new Job(Job.Script(resourceUri("annotToLD.py"), s"--sub-region=$subRegion", s"--ancestry=$output"))
  }

  /** Before the jobs actually run, perform this operation.
    */
  override def prepareJob(output: String): Unit = {
    context.s3.rm(s"out/ldsc/regions/${subRegion}/ld_score/ancestry=${output}/")
  }

  /** Update the success flag of the merged regions.
    */
  override def success(output: String): Unit = {
    context.s3.touch(s"out/ldsc/regions/${subRegion}/ld_score/ancestry=${output}/_SUCCESS")
    ()
  }
}
