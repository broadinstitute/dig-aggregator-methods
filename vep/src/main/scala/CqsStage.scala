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
  *  s3://dig-analysis-data/out/varianteffect/effects/part-*.json
  *
  * The output location:
  *
  *  s3://dig-analysis-data/out/varianteffect/cqs/part-*.json
  *
  * The inputs and outputs for this processor are expected to be phenotypes.
  */
class CqsStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val effects: Input.Source = Input.Source.Success("out/varianteffect/effects/")

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(effects)

  // EMR cluster to run the job steps on
  override def cluster: ClusterDef = super.cluster.copy(
    masterInstanceType = Strategy.generalPurpose(),
    slaveInstanceType = Strategy.generalPurpose(),
    instances = 4
  )

  /** Make inputs to the outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case effects() => Outputs.Named("cqs")
  }

  /** All effect results are combined together, so the results list is ignored. */
  override def make(output: String): Job = {
    new Job(Job.PySpark(resourceUri("cqs.py")))
  }
}
