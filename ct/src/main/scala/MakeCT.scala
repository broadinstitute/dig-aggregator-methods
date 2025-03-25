package org.broadinstitute.dig.aggregator.methods.ct

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

import org.broadinstitute.dig.aws.Ec2.Strategy
import org.broadinstitute.dig.aws.MemorySize

class MakeCT(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val ancestrySpecific: Input.Source = Input.Source.Success("out/credible_sets/intake/*/EU/bottom-line/")

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
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("install-ct.sh")))
  )

  override def make(output: String): Job = {
    val input = MakeCTInput.fromString(output)
    new Job(Job.Script(resourceUri("makeCT.py"), input.flags:_*))
  }

}
  
case class MakeCTInput(
  phenotype: String,
  ancestry: String
) {

  def flags: Seq[String] = Seq(s"--phenotype=$phenotype", s"--ancestry=$ancestry")
}

object MakeCTInput {
  def fromString(output: String): MakeCTInput = {
    output.split("/").toSeq match {
      case Seq(phenotype, ancestry) => MakeCTInput(phenotype, ancestry)
    }
  }
}

