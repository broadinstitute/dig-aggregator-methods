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
class AncestrySpecificStage(implicit context: Context) extends Stage {

  /** The set of unique ancestries to process.
    */
  private val ancestries = Seq("AA", "AF", "EU", "HS", "EA", "SA")

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

  /** Map any new datasets to process all ancestries. Since datasets aren't
    * uploaded to ancestry-specific locations, we actually can't tell from
    * the dataset which ancestries need processed, so we have to process
    * all of them.
    *
    * This can be fixed if - in addition to matching the `metadata` file in
    * S3 - we also provided the contents of the metadata file here so that
    * it can be examined. We'd see the ancestry listed there and limit what
    * is processed to that.
    */
  override val rules: PartialFunction[Input, Outputs] = {
    case variants(_, _) => Outputs.Named(ancestries: _*)
  }

  /** Create the job, each ancestry is its own output.
    */
  override def make(output: String): Job = {
    new Job(Job.PySpark(resourceUri("ancestrySpecific.py"), output))
  }
}
