package org.broadinstitute.dig.aggregator.methods.magma

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy

/** After meta-analysis, this stage finds the most significant variant
  * every 50 kb across the entire genome.
  */
class MagmaStep4GenePValuesStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val magmaStep4GenePValues: Input.Source = Input.Source.Success("out/magma/step3VariantPValues/*/")

  override def additionalResources: Seq[String] = Seq(
    "step4CalculateGenePValues.sh"
  )

  /** The output of meta-analysis is the input for top associations. */
  override val sources: Seq[Input.Source] = Seq(magmaStep4GenePValues)

  private lazy val step4Bootstrap = resourceUri("step4Bootstrap.sh")

  /** Process top associations for each phenotype. */
  override val rules: PartialFunction[Input, Outputs] = {
    case magmaStep4GenePValues(phenotype) => Outputs.Named(phenotype)
  }

  /** Simple cluster with more memory. */
  override val cluster: ClusterDef = super.cluster.copy(
    masterInstanceType = Strategy.generalPurpose(mem = 32.gb),
//    slaveInstanceType = Strategy.generalPurpose(mem = 32.gb),
//    instances = 4
//    masterInstanceType = Strategy.computeOptimized(vCPUs = 8),
    instances = 1,
    masterVolumeSizeInGB = 80,
    applications = Seq.empty,
    bootstrapScripts = Seq(new BootstrapScript(step4Bootstrap)),
  )

  /** Build the job. */
  override def make(output: String): Job = {
    val runScript = resourceUri("step4CalculateGenePValues.sh")

    // get all the variant part files to process, use only the part filename
    // get all the variant part files to process, use only the part filename
    val phenotype = output
    val objects = context.s3.ls(s"out/magma/step3VariantPValues/${phenotype}/")
    val parts   = objects.map(_.key.split('/').last).filter(_.startsWith("part-"))

    // add a step for each part file
    new Job(parts.take(1).map(Job.Script(runScript, _, phenotype)), isParallel = true)
  }

  /** Before the jobs actually run, perform this operation.
    */
  override def prepareJob(output: String): Unit = {
    val phenotype = output
    context.s3.rm(s"out/magma/step4GenePValues/${phenotype}/")
  }

  /** On success, write the _SUCCESS file in the output directory.
    */
  override def success(output: String): Unit = {
    val phenotype = output
    context.s3.touch(s"out/magma/step4GenePValues/${phenotype}/_SUCCESS")
    ()
  }

}
