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
  val variants: Input.Source     = Input.Source.Success("out/magma/staging/variants/")

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(associations)

  /** Process top associations for each phenotype. */
  override val rules: PartialFunction[Input, Outputs] = {
    case associations(phenotype, ancestry) => Outputs.Named(s"$phenotype/${ancestry.split("=").last}")
    case variants()              => Outputs.All
  }

  /** Simple cluster with more memory. */
  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    masterVolumeSizeInGB = 80,
    applications = Seq.empty,
    stepConcurrency = 5,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("installMagma.sh")))
  )

  /** Build the job. */
  override def make(output: String): Job = {
    val jobInput = GeneAssociationsInput.fromOutput(output)
    new Job(Job.Script(resourceUri("geneAssociations.sh"), jobInput.phenotype, jobInput.ancestry, jobInput.g1000Ancestry))
  }

  /** Before the jobs actually run, perform this operation.
    */
  override def prepareJob(output: String): Unit = {
    val jobInput = GeneAssociationsInput.fromOutput(output)
    context.s3.rm(s"out/magma/staging/genes/${jobInput.phenotype}/ancestry=${jobInput.ancestry}/")
  }

  /** On success, write the _SUCCESS file in the output directory.
    */
  override def success(output: String): Unit = {
    val jobInput = GeneAssociationsInput.fromOutput(output)
    context.s3.touch(s"out/magma/staging/genes/${jobInput.phenotype}/ancestry=${jobInput.ancestry}/_SUCCESS")
    ()
  }
}

case class GeneAssociationsInput(
  phenotype: String,
  ancestry: String,
  g1000Ancestry: String
)

case object GeneAssociationsInput {
  val ancestry_to_g1000: Map[String, String] = Map(
    "AA" -> "afr",
    "AF" -> "afr",
    "SSAF" -> "afr",
    "HS" -> "amr",
    "EA" -> "eas",
    "EU" -> "eur",
    "SA" -> "sas",
    "GME" -> "sas",
    "Mixed" -> "eur"
  )

  def fromOutput(output: String): GeneAssociationsInput = {
    output.split("/").toSeq match {
      case Seq(phenotype, ancestry) => GeneAssociationsInput(phenotype, ancestry, ancestry_to_g1000(ancestry))
    }
  }
}
