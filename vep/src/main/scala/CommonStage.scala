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
    masterInstanceType = Strategy.generalPurpose(),
    slaveInstanceType = Strategy.generalPurpose(),
    instances = 4
  )

  /** Make inputs to the outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case _ => Outputs.Named("common")
  }

  /** All effect results are combined together, so the results list is ignored. */
  override def make(output: String): Job = {
    new Job(Job.PySpark(resourceUri("common.py")))
  }
}
