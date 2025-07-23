package org.broadinstitute.dig.aggregator.methods.bioindex

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

/** The final result of all aggregator methods is building the BioIndex. All
  * outputs are to the dig-bio-index bucket in S3.
  */
class PhewasAssociationsStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val transEthnic: Input.Source = Input.Source.Success("out/metaanalysis/bottom-line/trans-ethnic/*/")
  val ancestrySpecific: Input.Source = Input.Source.Success("out/metaanalysis/bottom-line/ancestry-specific/*/*/")
  val clumped: Input.Source = Input.Source.Success("out/credible_sets/merged/*/*/")

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(transEthnic, ancestrySpecific, clumped)

  /** Rules for mapping input to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case transEthnic(_) => Outputs.Named("Mixed")
    case ancestrySpecific(_, ancestry) => Outputs.Named(ancestry.split("=").last)
    case clumped(_, ancestry) => Outputs.Named(ancestry)
  }

  /** Cluster with a lots of memory to prevent shuffling. */
  override val cluster: ClusterDef = super.cluster.copy(
    masterInstanceType = Ec2.Strategy.generalPurpose(mem = 64.gb),
    slaveInstanceType = Ec2.Strategy.generalPurpose(mem = 64.gb),
    masterVolumeSizeInGB = 250,
    slaveVolumeSizeInGB = 800,
    instances = 10,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("cluster-bootstrap.sh")))
  )

  /** Output to Job steps. */
  override def make(output: String): Job = {
    new Job(Job.PySpark(resourceUri("phewasAssociations.py"), s"--ancestry=$output"))
  }
}
