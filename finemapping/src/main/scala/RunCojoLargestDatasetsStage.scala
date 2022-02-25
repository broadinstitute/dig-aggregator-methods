package org.broadinstitute.dig.aggregator.methods.finemapping

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy

/** Run the cojo (clustering with conditional pValues) on all ancestry/phernotype combinations except Mixed
  */
class RunCojoLargestDatasetsStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val associations: Input.Source = Input.Source.Success("out/finemapping/variant-associations-largest-datasets/*/")
  val inputDirectory: String = "variant-associations-largest-datasets"
  val outputDirectory: String = "cojo-results-largest-datasets"

//  val variants: Input.Source     = Input.Source.Success("out/magma/staging/variants/")

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(associations)

  /** Process top associations for each phenotype. */
  override val rules: PartialFunction[Input, Outputs] = {
    case associations(phenotype) => Outputs.Named(phenotype)
//    case variants()              => Outputs.All
  }

  /** Additional resources to upload. */
  override def additionalResources: Seq[String] = Seq(
    "runCojo.py"
  )

  /** Simple cluster with more memory. */
  override val cluster: ClusterDef = super.cluster.copy(
//    masterInstanceType = Strategy.computeOptimized(vCPUs = 32),
    masterInstanceType = Ec2.Strategy.memoryOptimized(mem = 128.gb),
    instances = 1,
    masterVolumeSizeInGB = 80,
    applications = Seq.empty,
    stepConcurrency = 1,          // test on BMI/EUR showed 33% use of resources at max
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("installCojo.sh")))
  )

  /** Build the job. */
  override def make(output: String): Job = {
    // add in the python script 
    val cojoScript = resourceUri("runCojo.py")

    // run the job 
    new Job(Job.Script(resourceUri("runCojo.sh"), output, inputDirectory, outputDirectory))
  }

  /** Before the jobs actually run, perform this operation.
    */
  override def prepareJob(output: String): Unit = {
    context.s3.rm(s"out/finemapping/${outputDirectory}/${output}/")
  }

  /** On success, write the _SUCCESS file in the output directory.
    */
  override def success(output: String): Unit = {
    //TODO - should there be a success file in the ancestry directory
    context.s3.touch(s"out/finemapping/${outputDirectory}/${output}/_SUCCESS")
    ()
  }
}
