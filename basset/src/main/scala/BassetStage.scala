package org.broadinstitute.dig.aggregator.methods.basset

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy
import org.broadinstitute.dig.aws.MemorySize

class BassetStage(implicit context: Context) extends Stage {

  override val cluster: ClusterDef = super.cluster.copy(
    masterInstanceType = Strategy.computeOptimized(),
    instances = 1,
    masterVolumeSizeInGB = 60,
    stepConcurrency = 3,
    applications = Seq.empty,
    bootstrapScripts = Seq(
      new BootstrapScript(resourceUri("bassetBootstrap.sh"))
    )
  )

  val variants: Input.Source = Input.Source.Success("out/varianteffect/variants/")

  override val sources: Seq[Input.Source] = Seq(variants)

  override val rules: PartialFunction[Input, Outputs] = {
    case variants() => Outputs.Named("basset")
  }


  override def additionalResources: Seq[String] = Seq(
    "dcc_basset_lib.py",
    "fullBassetScript.py"
  )

  override def make(output: String): Job = {
    val bassetScript = resourceUri("bassetScript.sh")

    // get all the variant part files to process, use only the part filename
    val objects = context.s3.ls(s"out/varianteffect/variants/")
    val parts   = objects.map(_.key.split('/').last).filter(_.startsWith("part-"))
    val steps   = parts.map(part => Job.Script(bassetScript, part))

    // create the job; run steps in parallel
    new Job(steps, parallelSteps = true)
  }

  override def prepareJob(output: String): Unit = {
    context.s3.rm("out/basset/")
  }

  override def success(output: String): Unit = {
    context.s3.touch("out/basset/_SUCCESS")
    ()
  }
}
