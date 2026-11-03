package org.broadinstitute.dig.aggregator.methods.ldsc

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy
import org.broadinstitute.dig.aws.MemorySize


class MakeDatasetSumstatsStage(implicit context: Context) extends Stage {

  val datasets: Input.Source = Input.Source.Raw("out/metaanalysis/variants/*/*/*/*/part-00000*")

  /** Source inputs. */
  override val sources: Seq[Input.Source] = Seq(datasets)

  /** Map inputs to their outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case datasets(phenotype, dataset, ancestry, _, _) => Outputs.Named(s"$phenotype/${ancestry.split('=').last}/${dataset.split('=').last}")
  }

  /** Just need a single machine with no applications, but a good drive. */
  override def cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    applications = Seq.empty,
    masterVolumeSizeInGB = 100,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("install-sumstats.sh")))
  )

  override def make(output: String): Job = {
    val input = MakeDatasetSumstatsInput.fromString(output)
    new Job(Job.Script(resourceUri("makeDatasetSumstats.py"), input.flags:_*))
  }
}

case class MakeDatasetSumstatsInput(
  phenotype: String,
  ancestry: String,
  dataset: String
) {
  def flags: Seq[String] = Seq(s"--phenotype=$phenotype", s"--ancestry=$ancestry",  s"--dataset=$dataset")
}

object MakeDatasetSumstatsInput {
  def fromString(output: String): MakeDatasetSumstatsInput = {
    output.split("/").toSeq match {
      case Seq(phenotype, ancestry, dataset) => MakeDatasetSumstatsInput(phenotype, ancestry, dataset)
    }
  }
}

