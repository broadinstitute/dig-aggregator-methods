package org.broadinstitute.dig.aggregator.methods.basset

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

class TranslateBassetStage(implicit context: Context) extends Stage {

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    stepConcurrency = 5,
    applications = Seq.empty,
    bootstrapScripts = Seq(
      new BootstrapScript(resourceUri("translateBootstrap.sh"))
    ),
    releaseLabel = ReleaseLabel("emr-6.7.0")
  )

  val basset: Input.Source = Input.Source.Success("out/basset/variants/")

  override val sources: Seq[Input.Source] = Seq(basset)

  override val rules: PartialFunction[Input, Outputs] = {
    case basset() => Outputs.Named("basset")
  }

  override def make(output: String): Job = {
    // get all the variant part files to process, use only the part filename
    val objects = context.s3.ls(s"out/basset/variants/")
    val parts = objects.map(_.key.split('/').last).filter(_.startsWith("part-"))
    val steps = parts.map(part => Job.Script(resourceUri("translateBassetResults.py"), s"--part=$part"))

    // create the job; run steps in parallel
    new Job(steps, parallelSteps = true)
  }

  override def prepareJob(output: String): Unit = {
    context.s3.rm("out/basset/translated/")
  }

  override def success(output: String): Unit = {
    context.s3.touch("out/basset/translated/_SUCCESS")
    ()
  }
}
