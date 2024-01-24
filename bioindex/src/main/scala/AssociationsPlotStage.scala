package org.broadinstitute.dig.aggregator.methods.bioindex

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

/** The final result of all aggregator methods is building the BioIndex. All
 * outputs are to the dig-bio-index bucket in S3.
 */
class AssociationsPlotStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val transEthnic = Input.Source.Success("out/metaanalysis/trans-ethnic/*/")
  val ancestrySpecific = Input.Source.Success("out/metaanalysis/ancestry-specific/*/*/")

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(transEthnic, ancestrySpecific)

  /** Rules for mapping input to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case transEthnic(phenotype) => Outputs.Named(phenotype)
    case ancestrySpecific(phenotype, ancestry) =>
      Outputs.Named(s"$phenotype/${ancestry.split("ancestry=").last}")
  }

  /** Use memory-optimized machine with sizeable disk space for shuffling. */
  override val cluster: ClusterDef = super.cluster.copy(
    masterInstanceType = Ec2.Strategy.memoryOptimized(mem = 128.gb),
    masterVolumeSizeInGB = 200,
    instances = 1,
    applications = Seq.empty,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("cluster-bootstrap.sh")))
  )

  /** Output to Job steps. */
  override def make(output: String): Job = {
    val flags = output.split("/").toSeq match {
      case Seq(phenotype) => Seq(s"--phenotype=$phenotype", s"--ancestry=Mixed")
      case Seq(phenotype, ancestry) => Seq(s"--phenotype=$phenotype", s"--ancestry=$ancestry")
    }
    new Job(Job.Script(resourceUri("plotAssociations.py"), flags:_*))
  }
}
