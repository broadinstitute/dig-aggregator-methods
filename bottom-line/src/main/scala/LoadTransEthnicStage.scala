package org.broadinstitute.dig.aggregator.methods.bottomline

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy

class LoadTransEthnicStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._
  import org.broadinstitute.dig.aggregator.core.Implicits.S3Key

  val variants: Input.Source = Input.Source.Success("variants/*/*/*/")
  val transEthnicTables: Input.Source = Input.Source.Success("out/metaanalysis/bottom-line/staging/trans-ethnic/*/")

  // NOTE: Fairly slow, but this can run with a single m5.2xlarge if necessary
  // A cluster of 3 compute instances though makes it quite swift
  override val cluster: ClusterDef = super.cluster.copy(
    masterInstanceType = Strategy.computeOptimized(vCPUs = 8, mem = 16.gb),
    slaveInstanceType = Strategy.computeOptimized(vCPUs = 8, mem = 16.gb),
    instances = 4,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("cluster-bootstrap.sh"))),
    releaseLabel = ReleaseLabel("emr-6.7.0") // Need emr 6.1+ to read zstd files
  )

  /** Any new dataset means the trans-ethnic loading has to be run again.
   * Just checking for TE staging data isn't enough since a phenotype with only a Mixed dataset won't have staging data
   * Checking the tables ensures that everything is run in the proper order.
   * */
  override val sources: Seq[Input.Source] = Seq(variants, transEthnicTables)

  override val rules: PartialFunction[Input, Outputs] = {
    case variants(_, _, phenotype) => Outputs.Named(phenotype)
    case transEthnicTables(phenotype) => Outputs.Named(phenotype)
  }

  override def make(output: String): Job = {
    val loadAnalysis = resourceUri("loadAnalysis.py")

    new Job(Seq(Job.PySpark(
      loadAnalysis,
      "--trans-ethnic",
      "--phenotype", output
    )))
  }
}
