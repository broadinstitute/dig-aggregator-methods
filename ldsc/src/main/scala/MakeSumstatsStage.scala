package org.broadinstitute.dig.aggregator.methods.ldsc

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy
import org.broadinstitute.dig.aws.MemorySize

class MakeSumstatsStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val allDatasets: Input.Source = Input.Source.Dataset("variants/*/*/*/")

  /** Source inputs. */
  override val sources: Seq[Input.Source] = Seq(allDatasets)

  /** Map inputs to their outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case allDatasets(_, dataset, phenotype) => Outputs.Named(s"$dataset/$phenotype")
  }

  /** Just need a single machine with no applications, but a good drive. */
  override def cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    applications = Seq.empty,
    masterVolumeSizeInGB = 100,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("install-sumstats.sh"))),
    releaseLabel = ReleaseLabel("emr-6.7.0")
  )

  override def make(output: String): Job = {
    val input = MakeSumstatsInput.fromString(output)
    new Job(Job.Script(resourceUri("makeSumstats.py"), input.flags:_*))
  }
}

case class MakeSumstatsInput(
  dataset: String,
  phenotype: String
) {
  def flags: Seq[String] = Seq(s"--dataset=$dataset", s"--phenotype=$phenotype")
}

object MakeSumstatsInput {
  def fromString(output: String): MakeSumstatsInput = {
    output.split("/").toSeq match {
      case Seq(dataset, phenotype) => MakeSumstatsInput(dataset, phenotype)
    }
  }
}

