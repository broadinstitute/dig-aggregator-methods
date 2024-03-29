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
  *  s3://dig-analysis-data/out/varianteffect/snp/part-*.csv
  *
  * The inputs and outputs for this processor are expected to be phenotypes.
  */
class SnpStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  // import directly from the raw datasource
  val dbSNP: Input.Source = Input.Source.Raw("raw/dbSNP_common_GRCh37.vcf.gz")

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(dbSNP)

  /** Make inputs to the outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case _ => Outputs.Named("snp")
  }

  /** All effect results are combined together, so the results list is ignored. */
  override def make(output: String): Job = {
    new Job(Job.PySpark(resourceUri("snp.py")))
  }
}
