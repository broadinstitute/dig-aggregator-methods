package org.broadinstitute.dig.aggregator.methods.vep

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws.Ec2.Strategy
import org.broadinstitute.dig.aws.MemorySize
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.emr.configurations.{MapReduce, Spark}
import software.amazon.awssdk.services.s3.model.S3Object


class CqsStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val effects: Input.Source = Input.Source.Success("out/varianteffect/*/cqs-effects/")

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(effects)

  // EMR cluster to run the job steps on
  override def cluster: ClusterDef = super.cluster.copy(
    masterVolumeSizeInGB = 400,
    instances = 1,
    applications = Seq.empty,
    bootstrapScripts = Seq(
      new BootstrapScript(resourceUri("zstd-bootstrap.sh"))
    ),
    stepConcurrency = 3
  )

  /** Make inputs to the outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case effects(dataType) => Outputs.Named(dataType)
  }


  override def make(output: String): Job = {
    // get all the variant part files to process, use only the part filename
    val objects: List[S3Object] = context.s3.ls(s"out/varianteffect/$output/cqs-effects/")
    val parts: Seq[String] = objects.map(_.key.split('/').last).filter(_.startsWith("part-"))
    val scripts: Seq[Job.Script] = parts.map { part =>
      Job.Script(resourceUri("cqs.py"), s"--part=$part", s"--data-type=$output")
    }

    new Job(scripts, parallelSteps = true)
  }

  /** Before the jobs actually run, perform this operation.
    */
  override def prepareJob(output: String): Unit = {
    context.s3.rm(s"out/varianteffect/$output/cqs/")
  }

  /** Update the success flag of the merged regions.
    */
  override def success(output: String): Unit = {
    context.s3.touch(s"out/varianteffect/$output/cqs/_SUCCESS")
    ()
  }
}
