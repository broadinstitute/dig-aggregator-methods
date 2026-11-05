package org.broadinstitute.dig.aggregator.methods.ldsc

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy
import org.broadinstitute.dig.aws.MemorySize

class HeritabilityStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val sumstats: Input.Source = Input.Source.Raw("out/ldsc/sumstats/*/*/*.sumstats.gz")

  /** Source inputs. */
  override val sources: Seq[Input.Source] = Seq(sumstats)

  /** Map inputs to their outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case sumstats(phenotype, ancestry, _) => Outputs.Named(s"$phenotype/${ancestry.split('=').last}")
  }

  /** Just need a single machine with no applications, but a good drive. */
  override def cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    applications = Seq.empty,
    masterVolumeSizeInGB = 100,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("install-gc.sh"))),
    stepConcurrency = 5
  )

  override def make(output: String): Job = {
    val input = HeritabilityInput.fromString(output)
    new Job(Job.Script(resourceUri("runHeritability.py"), input.flags:_*))
  }
}

case class HeritabilityInput(
  phenotype: String,
  ancestry: String
) {
  def flags: Seq[String] = Seq(s"--phenotype=$phenotype", s"--ancestry=$ancestry")
}

object HeritabilityInput {
  def fromString(output: String): HeritabilityInput = {
    output.split("/").toSeq match {
      case Seq(phenotype, ancestry) => HeritabilityInput(phenotype, ancestry)
    }
  }
}


