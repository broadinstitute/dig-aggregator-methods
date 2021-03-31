package org.broadinstitute.dig.aggregator.methods.vep

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws.Ec2.Strategy
import org.broadinstitute.dig.aws.MemorySize
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.emr.configurations.{MapReduce, Spark}

/** After all the variants across all datasets have had VEP run on them in the
  * previous step the rsID for each variant is extracted into its own file.
  *
  * The input location:
  *
  *  s3://dig-analysis-data/out/varianteffect/cqs/part-*.json
  *  s3://dig-analysis-data/out/varianteffect/snp/part-*.json
  *
  * The output location:
  *
  *  s3://dig-analysis-data/out/varianteffect/common/part-*.json
  *
  * The inputs and outputs for this processor are expected to be phenotypes.
  */
class CommonStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val cqs: Input.Source = Input.Source.Success("out/varianteffect/effects/")
  val snp: Input.Source = Input.Source.Success("out/varianteffect/snp/")

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(cqs, snp)

  // EMR cluster to run the job steps on
  override def cluster: ClusterDef = super.cluster.copy(
    masterInstanceType = Strategy.memoryOptimized(mem=90.gb),
    instances = 1,
    stepConcurrency = 5
  )

  /** Make inputs to the outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case _ => Outputs.Named("common")
  }

  /** All effect results are combined together, so the results list is ignored. */
  override def make(output: String): Job = {
    val script = resourceUri("common.py")

    // get all the variant part files to process, use only the part filename
    val objects = context.s3.ls(s"out/varianteffect/effects/")
    val parts = objects.map(_.key)
      .filterNot(_.contains("/warnings/")
      .map(_.split('/').last)
      .filter(_.startsWith("part-"))

    // add a step for each part file
    new Job(parts.map(Job.Script(script, _)), parallelSteps = true)
  }

  /** Before the jobs actually run, perform this operation.
    */
  override def prepareJob(output: String): Unit = {
    context.s3.rm("out/varianteffect/common/")
  }
}
