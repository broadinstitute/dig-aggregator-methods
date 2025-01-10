package org.broadinstitute.dig.aggregator.methods.vep

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws.Ec2.Strategy
import org.broadinstitute.dig.aws.MemorySize
import org.broadinstitute.dig.aws.emr._
import software.amazon.awssdk.services.s3.model.S3Object


class CQSVepStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val variants: Input.Source = Input.Source.Success("out/varianteffect/*/variants/")

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(variants)

  private lazy val clusterBootstrap = resourceUri("cluster-bootstrap.sh")
  private lazy val installScript    = resourceUri("installCQSVep.sh")
  private lazy val zstdBootstrap = resourceUri("zstd-bootstrap.sh")

  /** Definition of each VM "cluster" of 1 machine that will run VEP.
    */
  override def cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    masterInstanceType = Strategy.generalPurpose(vCPUs=16),
    masterVolumeSizeInGB = 800,
    applications = Seq.empty,
    bootstrapScripts = Seq(
      new BootstrapScript(clusterBootstrap),
      new BootstrapScript(installScript),
      new BootstrapScript(zstdBootstrap)
    ),
    stepConcurrency = 4
  )

  /** Map inputs to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case variants(dataType) => Outputs.Named(dataType)
  }

  /** The results are ignored, as all the variants are refreshed and everything
    * needs to be run through VEP again.
    */
  override def make(output: String): Job = {
    // get all the variant part files to process, use only the part filename
    val objects: List[S3Object] = context.s3.ls(s"out/varianteffect/$output/variants/")
    val parts: Seq[String] = objects.map(_.key.split('/').last).filter(_.startsWith("part-"))
    val scripts: Seq[Job.Script] = parts.map { part =>
      Job.Script(resourceUri("runCQSVep.sh"), part, output)
    }

    new Job(scripts, parallelSteps = true)
  }

  /** Before the jobs actually run, perform this operation.
    */
  override def prepareJob(output: String): Unit = {
    context.s3.rm(s"out/varianteffect/$output/cqs-effects/")
    context.s3.rm(s"out/varianteffect/$output/cqs-warnings/")
  }

  /** On success, write the _SUCCESS file in the output directory.
    */
  override def success(output: String): Unit = {
    context.s3.touch(s"out/varianteffect/$output/cqs-effects/_SUCCESS")
    ()
  }
}
