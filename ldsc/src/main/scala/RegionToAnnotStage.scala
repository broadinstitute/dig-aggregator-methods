package org.broadinstitute.dig.aggregator.methods.ldsc

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws.emr._

class RegionToAnnotStage(implicit context: Context) extends Stage {

  val partitions: Seq[String] = Seq()
  val subRegion: String = if (partitions.isEmpty) "default" else partitions.mkString("-")
  val mergedFiles: Input.Source = Input.Source.Success(s"out/ldsc/regions/${subRegion}/merged/*/")

  /** Source inputs. */
  override val sources: Seq[Input.Source] = Seq(mergedFiles)

  /** Just need a single machine with no applications, but a good drive. */
  override def cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    applications = Seq.empty,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("install-ldscore.sh"))),
    releaseLabel = ReleaseLabel("emr-6.7.0"),
    stepConcurrency = 7
  )

  override val rules: PartialFunction[Input, Outputs] = {
    case mergedFiles(region) => Outputs.Named(region)
  }

  /** The partition names are combined together across datasets into single
    * BED files that can then be read by GREGOR.
    */
  override def make(output: String): Job = {
    new Job(Job.Script(resourceUri("regionsToAnnot.py"), s"--sub-region=$subRegion", s"--region-name=$output"))
  }

  /** Before the jobs actually run, perform this operation.
    */
  override def prepareJob(output: String): Unit = {
    context.s3.rm(s"out/ldsc/regions/${subRegion}/annot/${output}/")
  }

  /** Update the success flag of the merged regions.
    */
  override def success(output: String): Unit = {
    context.s3.touch(s"out/ldsc/regions/${subRegion}/annot/${output}/_SUCCESS")
    ()
  }
}
