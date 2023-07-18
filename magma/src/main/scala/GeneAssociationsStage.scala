package org.broadinstitute.dig.aggregator.methods.magma

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy

/** After meta-analysis, this stage finds the most significant variant
  * every 50 kb across the entire genome.
  */
class GeneAssociationsStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val associations: Input.Source = Input.Source.Success("out/magma/variant-associations/*/*/")

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(associations)

  /** Process top associations for each phenotype. */
  override val rules: PartialFunction[Input, Outputs] = {
    case associations(phenotype, ancestry_or_dataset) => Outputs.Named(s"$phenotype/$ancestry_or_dataset")
  }

  /** Simple cluster with more memory. */
  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    masterVolumeSizeInGB = 80,
    applications = Seq.empty,
    stepConcurrency = 5,
    bootstrapScripts = Seq(
      new BootstrapScript(resourceUri("installMagma.sh")),
      new BootstrapScript(resourceUri("installRDSPackages.sh"))
      )
  )

  override def additionalResources: Seq[String] = Seq(
    "geneAssociations.sh"
  )

  /** Build the job. */
  override def make(output: String): Job = {
    val input = GeneAssociationsInput.fromOutput(output)
    new Job(Job.Script(resourceUri("geneAssociations.py"), input.flags:_*))
  }
}

case class GeneAssociationsInput(
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

case object GeneAssociationsInput {
  def fromOutput(output: String): GeneAssociationsInput = {
    output.split("/").toSeq match {
      case Seq(phenotype, ancestry) if ancestry.contains("ancestry=") =>
        GeneAssociationsInput(phenotype, Some(ancestry.split("=").last), None)
      case Seq(phenotype, dataset) if dataset.contains("dataset=") =>
        GeneAssociationsInput(phenotype, None, Some(dataset.split("=").last))
    }
  }
}
