package org.broadinstitute.dig.aggregator.methods.burdenbinning

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

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
class BurdenbinningStage(implicit context: Context) extends Stage {

  /** Cluster configuration used when running this stage. The super
    * class already has a default configuration defined, so it's easier
    * to just copy and override specific parts of it.
    */
  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    bootstrapScripts = Seq(
      new BootstrapScript(resourceUri("sampleBootstrap.sh"))
    )
  )

  /** Input sources need to be declared so they can be used in rules.
    *
    * Input sources are a glob-like S3 prefix to an object in S3. Wildcards
    * can be pattern matched in the rules of the stage.
    */
  val variants: Input.Source = Input.Source.Dataset("variants/*/*/")

  /** When run, all the input sources here will be checked to see if they
    * are new or updated.
    */
  override val sources: Seq[Input.Source] = Seq(variants)

  /** For every input that is new/updated, this partial function is called,
    * which pattern matches it against the inputs sources defined above and
    * maps them to the output(s) that should be built.
    *
    * In our variants input source, there are two wildcards in the S3 prefix,
    * which are matched to the dataset name and phenotype. The dataset is
    * ignored, and the name of the phenotype is used as the output.
    */
  override val rules: PartialFunction[Input, Outputs] = {
    case variants(dataset, phenotype) => Outputs.Named(phenotype)
  }

  /** Once all the rules have been applied to the new and updated inputs,
    * each of the outputs that needs built is send here. A job is returned,
    * which is the series of steps that need to be executed on the cluster
    * in order for the final output to be successfully built.
    *
    * It is assumed that all outputs for a given stage are independent of
    * each other and can be executed in parallel across multiple, identical
    * clusters.
    */
  override def make(output: String): Job = {

    /* All job steps require a URI to a location in S3 where the script can
     * be read from by the cluster.
     *
     * The resourceUri function will upload the resource in the jar to a
     * unique location in S3 and return the URI to where it was uploaded.
     */
    val sampleSparkJob = resourceUri("sampleSparkJob.py")
    val sampleScript   = resourceUri("sampleScript.sh")

    // we used the phenotype as the output in rules
    val phenotype = output

    // list of steps to execute for this job
    val steps = Seq(
      Job.PySpark(sampleSparkJob, phenotype),
      Job.Script(sampleScript, phenotype)
    )

    // create the job
    new Job(steps)
  }
}
