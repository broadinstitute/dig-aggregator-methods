package org.broadinstitute.dig.aggregator.methods.magma

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy

/** After meta-analysis, this stage finds the most significant variant
  * every 50 kb across the entire genome.
  */
class VariantAssociationsStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val ancestrySpecific: Input.Source = Input.Source.Success("out/metaanalysis/ancestry-specific/*/*/")
  val datasets: Input.Source = Input.Source.Success("variants/*/*/*/")

  /** Ouputs to watch. */
  override val sources: Seq[Input.Source] = Seq(ancestrySpecific, datasets)

  /** Process top associations for each phenotype. */
  override val rules: PartialFunction[Input, Outputs] = {
    case ancestrySpecific(phenotype, ancestry) => Outputs.Named(s"ancestry/$phenotype/${ancestry.split("=").last}")
    case datasets(tech, dataset, phenotype) if tech == "GWAS" => Outputs.Named(s"dataset/$phenotype/$dataset")
    case datasets(tech, _, _) if tech != "GWAS" => Outputs.Null
  }

  /** Simple cluster with more memory. */
  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    releaseLabel = ReleaseLabel("emr-6.7.0")
  )

  /** Build the job. */
  override def make(output: String): Job = {
    val input = VariantAssociationsInput.fromString(output)
    new Job(Job.PySpark(resourceUri("variantAssociations.py"), input.flags:_*))
  }
}

case class VariantAssociationsInput(
  phenotype: String,
  maybeAncestry: Option[String],
  maybeDataset: Option[String]
) {
  def flags: Seq[String] = Seq(
    Some(s"--phenotype=$phenotype"),
    maybeAncestry.map(ancestry => s"--ancestry=$ancestry"),
    maybeDataset.map(dataset => s"--dataset=$dataset")
  ).flatten
}

object VariantAssociationsInput {
  def fromString(output: String): VariantAssociationsInput = {
    output.split("/").toSeq match {
      case Seq(inputType, phenotype, ancestry) if inputType == "ancestry" =>
        VariantAssociationsInput(phenotype, Some(ancestry), None)
      case Seq(inputType, phenotype, dataset) if inputType == "dataset" =>
        VariantAssociationsInput(phenotype, None, Some(dataset))
    }
  }
}
