package org.broadinstitute.dig.aggregator.methods.frequency

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
class CombinedFrequencyStage(implicit context: Context) extends Stage {

  /** Input sources need to be declared so they can be used in rules.
    *
    * Input sources are a glob-like S3 prefix to an object in S3. Wildcards
    * can be pattern matched in the rules of the stage.
    */
  val ancestry: Input.Source = Input.Source.Success("out/frequencyanalysis/*/")

  /** When run, all the input sources here will be checked to see if they
    * are new or updated.
    */
  override val sources: Seq[Input.Source] = Seq(ancestry)

  /** Any time an ancestry is updated, we need to reprocess all the combined.
    */
  override val rules: PartialFunction[Input, Outputs] = {
    case ancestry(_) => Outputs.Named("Combined")
  }

  /** Create the job, each ancestry is its own output.
    */
  override def make(output: String): Job = {
    new Job(Job.PySpark(resourceUri("combinedFreq.py")))
  }
}
