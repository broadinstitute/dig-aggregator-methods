package org.broadinstitute.dig.aggregator.methods.finemap

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy
import org.broadinstitute.dig.aws.MemorySize

class MakeFinemap(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val ancestrySpecific: Input.Source = Input.Source.Success("out/metaanalysis/bottom-line/ancestry-specific/*/ancestry=EU/")

  /** Source inputs. */
  override val sources: Seq[Input.Source] = Seq(ancestrySpecific)

  /** Map inputs to their outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case ancestrySpecific(phenotype) => Outputs.Named(s"$phenotype/EU")
    // case ancestrySpecific(phenotype, ancestry) => Outputs.Named(s"$phenotype/${ancestry.split('=').last}")
    // case mixedDatasets(_, _, phenotype) => Outputs.Named(s"$phenotype/Mixed")
  }

  /** Just need a single machine with no applications, but a good drive. */
  override def cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    applications = Seq.empty,
    masterVolumeSizeInGB = 100,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("install-finemap.sh")))
  )

  override def make(output: String): Job = {
    val input = MakeFinemapInput.fromString(output)
    new Job(Job.Script(resourceUri("makeFinemap.py"), input.flags:_*))
  }

}
  

case class MakeFinemapInput(
  phenotype: String,
  ancestry: String
) {

  def flags: Seq[String] = Seq(s"--phenotype=$phenotype", s"--ancestry=$ancestry")
}

object MakeFinemapInput {
  def fromString(output: String): MakeFinemapInput = {
    output.split("/").toSeq match {
      case Seq(phenotype, ancestry) => MakeFinemapInput(phenotype, ancestry)
    }
  }
}

