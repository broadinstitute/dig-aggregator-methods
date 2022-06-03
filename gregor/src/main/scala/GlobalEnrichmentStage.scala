package org.broadinstitute.dig.aggregator.methods.gregor

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws.Ec2.Strategy
import org.broadinstitute.dig.aws.MemorySize
import org.broadinstitute.dig.aws.emr.{BootstrapScript, ClusterDef, Job}

class GlobalEnrichmentStage(implicit context: Context) extends Stage {
  val partitions: Seq[String] = Seq()
  val subRegion: String = if (partitions.isEmpty) "default" else partitions.mkString("-")
  val regions: Input.Source = Input.Source.Success(s"out/ldsc/regions/$subRegion/merged/")
  val snp: Input.Source     = Input.Source.Success("out/gregor/snp/*/")

  /** All the processors this processor depends on.
    */
  override val sources: Seq[Input.Source] = Seq(regions, snp)

  /* Install scripts. */
  private lazy val bootstrap = resourceUri("cluster-bootstrap.sh")
  private lazy val install   = resourceUri("installGREGOR.sh")

  /* r^2 parameter to scripts - this should match bottom-line's plink param */
  private val r2 = "0.7"

  // cluster configuration used to process each phenotype
  override def cluster: ClusterDef = super.cluster.copy(
    masterInstanceType = Strategy.computeOptimized(),
    instances = 1,
    masterVolumeSizeInGB = 800,
    stepConcurrency = 5,
    bootstrapScripts = Seq(
      new BootstrapScript(bootstrap),
      new BootstrapScript(install, r2)
    )
  )

  // map internal ancestries to that of GREGOR/1000g
  private val ancestries = List(
    "AA" -> "AFR",
    "HS" -> "AMR",
    "EA" -> "ASN",
    "EU" -> "EUR",
    "SA" -> "SAN"
  )

  /** The outputs from the SNPListProcessor (phenotypes) are the outputs of this
    * processor, but all the sorted regions are processed with each as well.
    */
  override val rules: PartialFunction[Input, Outputs] = {
    case regions()      => Outputs.All
    case snp(phenotype) => Outputs.Named(phenotype)
  }

  /** Run GREGOR over the results of the SNP list and regions.
    */
  override def make(output: String): Job = {
    val run       = resourceUri("runGREGOR.sh")
    val phenotype = output

    // a phenotype needs processed per ancestry (can be run in parallel)
    val steps = ancestries.map {
      case (t2dkp_ancestry, gregor_ancestry) =>
        Job.Script(run, gregor_ancestry, r2, phenotype, t2dkp_ancestry)
    }

    // append the final step
    new Job(steps)
  }

  /** Before the jobs actually run, perform this operation.
    */
  override def prepareJob(output: String): Unit = {
    context.s3.rm(s"out/gregor/summary/${output}/")
  }

  /** Update the success flag of the merged regions.
    */
  override def success(output: String): Unit = {
    context.s3.touch(s"out/gregor/summary/${output}/_SUCCESS")
    ()
  }
}
