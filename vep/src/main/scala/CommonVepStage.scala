package org.broadinstitute.dig.aggregator.methods.vep

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws.Ec2.Strategy
import org.broadinstitute.dig.aws.MemorySize
import org.broadinstitute.dig.aws.emr._


class CommonVepStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._
  val variants: Input.Source = Input.Source.Success("out/varianteffect/variants/")

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(variants)

  private lazy val clusterBootstrap = resourceUri("cluster-bootstrap.sh")
  private lazy val installScript    = resourceUri("installCommonVEP.sh")

  /** Definition of each VM "cluster" of 1 machine that will run VEP.
   */
  override def cluster: ClusterDef = super.cluster.copy(
    masterInstanceType = Strategy.generalPurpose(vCPUs=16),
    instances = 1,
    masterVolumeSizeInGB = 100,
    applications = Seq.empty,
    bootstrapScripts = Seq(
      new BootstrapScript(clusterBootstrap),
      new BootstrapScript(installScript)
    ),
    stepConcurrency = 4
  )

  /** Map inputs to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case variants() => Outputs.Named("VEP")
  }

  /** The results are ignored, as all the variants are refreshed and everything
   * needs to be run through VEP again.
   */
  override def make(output: String): Job = {
    val runScript = resourceUri("runCommonVep.sh")

    // get all the variant part files to process, use only the part filename
    val objects = context.s3.ls(s"out/varianteffect/variants/")
    val parts   = objects.map(_.key.split('/').last).filter(_.startsWith("part-"))

    // add a step for each part file
    new Job(parts.map(Job.Script(runScript, _)), parallelSteps = true)
  }

  /** Before the jobs actually run, perform this operation.
   */
  override def prepareJob(output: String): Unit = {
    context.s3.rm("out/varianteffect/common-effects/")
    context.s3.rm("out/varianteffect/common-warnings/")
  }

  /** On success, write the _SUCCESS file in the output directory.
   */
  override def success(output: String): Unit = {
    context.s3.touch("out/varianteffect/common-effects/_SUCCESS")
    ()
  }
}
