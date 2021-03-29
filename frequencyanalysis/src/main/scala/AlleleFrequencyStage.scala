package org.broadinstitute.dig.aggregator.methods.frequencyanalysis

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

/** Take all loaded datasets and group by variant id to find the maximum
  * minor allele frequency for each variant.
  */
class AlleleFrequencyStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  /** Cluster configuration used when running this stage. The super
    * class already has a default configuration defined, so it's easier
    * to just copy and override specific parts of it.
    */
  override val cluster: ClusterDef = super.cluster.copy(
    masterInstanceType = Ec2.Strategy.memoryOptimized(mem = 90.gb),
    slaveInstanceType = Ec2.Strategy.memoryOptimized(mem = 90.gb),
    instances = 5
  )

  /** Input sources need to be declared so they can be used in rules.
    *
    * Input sources are a glob-like S3 prefix to an object in S3. Wildcards
    * can be pattern matched in the rules of the stage.
    */
  val variants: Input.Source = Input.Source.Dataset("variants/*/*/*/")

  /** When run, all the input sources here will be checked to see if they
    * are new or updated.
    */
  override val sources: Seq[Input.Source] = Seq(variants)

  /** Join all datasets together into a single job.
    */
  override val rules: PartialFunction[Input, Outputs] = {
    case variants(tech, dataset, phenotype) => Outputs.Named("maf")
  }

  /** Simple job to find max MAF across all datasets for each variant.
    */
  override def make(output: String): Job = {
    new Job(Job.PySpark(resourceUri("alleleFreq.py")))
  }
}
