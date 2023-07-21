package org.broadinstitute.dig.aggregator.methods.vep

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

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
  val dbSNP_common: Input.Source = Input.Source.Raw("raw/dbSNP_common_GRCh37.vcf.gz")
  val dbSNP: Input.Source = Input.Source.Raw("raw/dbSNP_full_GRCh37.vcf.gz")

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(dbSNP_common, dbSNP)

  /** Make inputs to the outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case dbSNP_common() => Outputs.Named("snp")
    case dbSNP() => Outputs.Named("snp_full")
  }

  // EMR cluster to run the job steps on
  override def cluster: ClusterDef = super.cluster.copy(
    masterInstanceType = Ec2.Strategy.generalPurpose(mem = 64.gb),
    slaveInstanceType = Ec2.Strategy.generalPurpose(mem = 64.gb),
    masterVolumeSizeInGB = 250,
    slaveVolumeSizeInGB = 300,
    instances = 3
  )

  /** All effect results are combined together, so the results list is ignored. */
  override def make(output: String): Job = {
    val flags: Seq[String] = if (output == "snp") {
      Seq("--fname=dbSNP_common_GRCh37.vcf.gz", "--output=snp")
    } else {
      Seq("--fname=dbSNP_full_GRCh37.vcf.gz", "--output=snp_full")
    }
    new Job(Job.PySpark(resourceUri("snp.py"), flags: _*))
  }
}
