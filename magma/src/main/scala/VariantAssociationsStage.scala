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
  val mixedDatasets: Input.Source = Input.Source.Dataset("variants/*/*/*/")
  val snps: Input.Source = Input.Source.Success("out/varianteffect/snp/")

  /** Ouputs to watch. */
  override val sources: Seq[Input.Source] = Seq(ancestrySpecific, mixedDatasets)

  /** Process top associations for each phenotype. */
  override val rules: PartialFunction[Input, Outputs] = {
    case ancestrySpecific(phenotype, ancestry) => Outputs.Named(s"$phenotype/${ancestry.split("=").last}")
    case mixedDatasets(_, _, phenotype) => Outputs.Named(s"$phenotype/Mixed")
  }

  /** Simple cluster with more memory. */
  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("installPythonPackages.sh")))
  )

  /** Build the job. */
  override def make(output: String): Job = {
    val input = VariantAssociationsInput.fromString(output)
    new Job(Job.PySpark(resourceUri("variantAssociations.py"), input.flags:_*))
  }

  /** Before the jobs actually run, perform this operation.
   */
  override def prepareJob(output: String): Unit = {
    val input = VariantAssociationsInput.fromString(output)
    context.s3.rm(input.outputDirectory + "/")
  }

  /** On success, write the _SUCCESS file in the output directory.
   */
  override def success(output: String): Unit = {
    val input = VariantAssociationsInput.fromString(output)
    context.s3.touch(input.outputDirectory + "/_SUCCESS")
    ()
  }
}

case class VariantAssociationsInput(
  phenotype: String,
  ancestry: String
) {
  def outputDirectory: String = s"out/magma/variant-associations/$phenotype/ancestry=$ancestry"

  def flags: Seq[String] = Seq(s"--phenotype=$phenotype", s"--ancestry=$ancestry")
}

object VariantAssociationsInput {
  def fromString(output: String): VariantAssociationsInput = {
    output.split("/").toSeq match {
      case Seq(phenotype, ancestry) => VariantAssociationsInput(phenotype, ancestry)
    }
  }
}
