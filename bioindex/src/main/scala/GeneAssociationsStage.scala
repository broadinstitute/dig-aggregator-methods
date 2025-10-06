package org.broadinstitute.dig.aggregator.methods.bioindex

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.config.emr._
import org.broadinstitute.dig.aws.emr._

/** The final result of all aggregator methods is building the BioIndex. All
  * outputs are to the dig-bio-index bucket in S3.
  */
class GeneAssociationsStage(implicit context: Context) extends Stage {
  val magma   = Input.Source.Success("out/magma/gene-associations/*/")
  val combined = Input.Source.Success("gene_associations/combined/*/")
  val traits_600 = Input.Source.Dataset("gene_associations/600k_600traits/*/")
  val transcript = Input.Source.Raw("transcript_associations/55k/*/part-*")

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(magma, combined, traits_600, transcript)

  /** Rules for mapping input to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case combined(phenotype) => Outputs.Named("combined")
    case traits_600(phenotype) => Outputs.Named("600trait")
    case magma(phenotype)   => Outputs.Named("magma")
    case transcript(phenotype, file) => Outputs.Named("transcript")
  }

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 6,
    masterVolumeSizeInGB = 100,
    slaveVolumeSizeInGB = 100,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("cluster-bootstrap.sh")))
  )

  /** Output to Job steps. */
  override def make(output: String): Job = {
    val script = resourceUri("geneAssociations.py")

    val step = output match {
      case "combined"   => Job.PySpark(script, "--combined")
      case "600trait" => Job.PySpark(script, "--600trait")
      case "magma" => Job.PySpark(script, "--magma")
      case "transcript" => Job.PySpark(script, "--transcript")
    }

    new Job(step)
  }
}
