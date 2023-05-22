package org.broadinstitute.dig.aggregator.methods.bottomline

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy

class TransEthnicStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val ancestrySpecific: Input.Source = Input.Source.Success("out/metaanalysis/ancestry-specific/*/")

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    applications = Seq.empty,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("cluster-bootstrap.sh"))),
    releaseLabel = ReleaseLabel("emr-6.7.0") // Need emr 6.1+ to read zstd files
  )

  /** Look for new datasets. */
  override val sources: Seq[Input.Source] = Seq(ancestrySpecific)

  /** There is an output made for each phenotype. */
  override val rules: PartialFunction[Input, Outputs] = {
    case ancestrySpecific(phenotype) => Outputs.Named(phenotype)
  }

  /** First partition all the variants across datasets (by dataset), then
   * run the ancestry-specific analysis and load it from staging. Finally,
   * run the trans-ethnic analysis and load the resulst from staging.
   */
  override def make(output: String): Job = {
    val transEthnic = resourceUri("runTransEthnic.sh")

    new Job(Seq(Job.Script(transEthnic, output)))
  }
}
