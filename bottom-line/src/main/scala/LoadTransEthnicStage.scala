package org.broadinstitute.dig.aggregator.methods.bottomline

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy

class LoadTransEthnicStage(implicit context: Context) extends Stage {

  val variants: Input.Source = Input.Source.Success("variants/*/*/*/")
  val transEthnicTables: Input.Source = Input.Source.Success("out/metaanalysis/staging/trans-ethnic/*/")

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("cluster-bootstrap.sh"))),
    releaseLabel = ReleaseLabel("emr-6.7.0") // Need emr 6.1+ to read zstd files
  )

  /** Any new dataset means the trans-ethnic loading has to be run again.
   * Just checking for TE staging data isn't enough since a phenotype with only a Mixed dataset won't have staging data
   * Checking the tables ensures that everything is run in the proper order.
   * */
  override val sources: Seq[Input.Source] = Seq(variants, transEthnicTables)

  /** There is an output made for each phenotype. */
  override val rules: PartialFunction[Input, Outputs] = {
    case variants(_, _, phenotype) => Outputs.Named(phenotype)
    case transEthnicTables(phenotype) => Outputs.Named(phenotype)
  }

  /** First partition all the variants across datasets (by dataset), then
   * run the ancestry-specific analysis and load it from staging. Finally,
   * run the trans-ethnic analysis and load the resulst from staging.
   */
  override def make(output: String): Job = {
    val loadAnalysis = resourceUri("loadAnalysis.py")

    new Job(Seq(Job.PySpark(
      loadAnalysis,
      "--trans-ethnic",
      "--phenotype", output
    )))
  }
}
