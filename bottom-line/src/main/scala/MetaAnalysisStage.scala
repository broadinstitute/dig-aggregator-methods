package org.broadinstitute.dig.aggregator.methods.bottomline

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy

/** Runs meta-analysis per phenotype across all datasets.
  *
  * Inputs:
  *
  *   s3://dig-analysis-data/variants/.../<phenotype>/part-*.json
  *
  * Outputs:
  *
  *   s3://dig-analysis-data/out/metaanalysis/variants/<phenotype>/...
  *
  *   s3://dig-analysis-data/out/metaanalysis/staging/trans-ethnic/<phenotype>/...
  *   s3://dig-analysis-data/out/metaanalysis/staging/ancestry-specific/<phenotype>/...
  *
  *   s3://dig-analysis-data/out/metaanalysis/ancestry-specific/<phenotype>/...
  *   s3://dig-analysis-data/out/metaanalysis/trans-ethnic/<phenotype>/...
  *
  * The variants path contains all the variants across all datasets for METAL,
  * partitioned by the dataset.
  *
  * The staging path contains the actual output of METAL, uploaded as a temporary
  * file, but useful for posterity.
  *
  * The ancestry-specific path contains the output of METAL per ancestry.
  *
  * The trans-ethnic path contains the output of METAL across ancestries.
  */
class MetaAnalysisStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val variants: Input.Source = Input.Source.Success("furkan/variants/*/*/*/")

  /** The master node - the one that actually runs METAL - needs a pretty
    * sizeable hard drive and more CPUs to download all the variants and run
    * them efficiently. The load steps can end up performing an RDD on LOTS
    * of data, so memory optimized instances are helpful.
    */
  override val cluster: ClusterDef = super.cluster.copy(
    masterInstanceType = Strategy.computeOptimized(vCPUs = 32),
    slaveInstanceType = Strategy.memoryOptimized(mem = 128.gb),
    masterVolumeSizeInGB = 800,
    instances = 4,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("cluster-bootstrap.sh")))
  )

  /** Additional resources to upload. */
  override def additionalResources: Seq[String] = Seq(
    "runMETAL.sh",
    "getmerge-strip-headers.sh"
  )

  /** Look for new datasets. */
  override val sources: Seq[Input.Source] = Seq(variants)

  /** There is an output made for each phenotype. */
  override val rules: PartialFunction[Input, Outputs] = {
    case variants(tech, dataset, phenotype) => Outputs.Named(phenotype)
  }

  /** First partition all the variants across datasets (by dataset), then
    * run the ancestry-specific analysis and load it from staging. Finally,
    * run the trans-ethnic analysis and load the resulst from staging.
    */
  override def make(output: String): Job = {
    val partition        = resourceUri("partitionVariants.py")
    val ancestrySpecific = resourceUri("runAncestrySpecific.sh")
    val transEthnic      = resourceUri("runTransEthnic.sh")
    val loadAnalysis     = resourceUri("loadAnalysis.py")
    val phenotype        = output

    // steps run serially
    val steps = Seq(
      Job.PySpark(partition, phenotype),
      // ancestry-specific analysis first and load it back
      Job.Script(ancestrySpecific, phenotype),
      Job.PySpark(loadAnalysis, "--ancestry-specific", phenotype)
//      // trans-ethnic next using ancestry-specific results
//      Job.Script(transEthnic, phenotype),
//      Job.PySpark(loadAnalysis, "--trans-ethnic", phenotype)
    )

    new Job(steps)
  }

  /** Nuke the staging directories before the job runs.
    */
  override def prepareJob(output: String): Unit = {
    context.s3.rm(s"furkan/out/metaanalysis/staging/ancestry-specific/$output/")
//    context.s3.rm(s"furkan/out/metaanalysis/staging/trans-ethnic/$output/")
  }
}
