package org.broadinstitute.dig.aggregator.methods.bottomline

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy

/** This is a stage in your method.
  *
  * Stages take one or more inputs and generate one or more outputs. Each
  * stage consists of a...
  *
  *   - list of input sources;
  *   - rules mapping inputs to outputs;
  *   - make function that returns a job used to produce a given output
  *
  * Optionally, a stage can also override...
  *
  *   - its name, which defaults to its class name
  *   - the cluster definition used to provision EC2 instances
  */
class ClumpingStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  /** The master node needs to be larger for plink.
    */
  override val cluster: ClusterDef = super.cluster.copy(
    masterInstanceType = Strategy.memoryOptimized(mem = 64.gb),
    instances = 1,
    masterVolumeSizeInGB = 400,
    bootstrapSteps = Seq(Job.Script(resourceUri("install-plink.sh")))
  )

  /** The output of the trans-ethnic bottom-line is the input.
    */
  val bottomLine: Input.Source = Input.Source.Success("out/metaanalysis/trans-ethnic/*/")
  val snps: Input.Source       = Input.Source.Success("out/varianteffect/snp/")

  /** When run, all the input sources here will be checked to see if they
    * are new or updated.
    */
  override val sources: Seq[Input.Source] = Seq(bottomLine, snps)

  /** Each phenotype is an output.
    */
  override val rules: PartialFunction[Input, Outputs] = {
    case bottomLine(phenotype) => Outputs.Named(phenotype)
    case snps()                => Outputs.All
  }

  /** Build the job to process a single phenotype.
    */
  override def make(output: String): Job = {
    //val variants  = resourceUri("variants.py")
    val runPlink  = resourceUri("runPlink.py")
    val phenotype = output

    new Job(Job.Script(runPlink, phenotype))
  }

  /** Nuke the staging directory before the job runs.
    */
  override def prepareJob(output: String): Unit = {
    context.s3.rm(s"out/metaanalysis/clumped/$output/")
  }

  /** Update the success flag of the top associations.
    */
  override def success(output: String): Unit = {
    context.s3.touch(s"out/metaanalysis/clumped/$output/_SUCCESS")
    ()
  }
}
