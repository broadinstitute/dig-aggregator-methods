package org.broadinstitute.dig.aggregator.methods.bottomline

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy

/** After meta-analysis, this stage finds the most significant variant
  * every 50 kb across the entire genome.
  */
class ClumpedAssociationsStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val transEthnic = Input.Source.Success("out/metaanalysis/trans-ethnic/*/")
  val ancestrySpecific = Input.Source.Success("out/metaanalysis/ancestry-specific/*/*/")
  val snps: Input.Source       = Input.Source.Success("out/varianteffect/snp/")

  /** The output of meta-analysis is the input for top associations. */
  override val sources: Seq[Input.Source] = Seq(transEthnic, ancestrySpecific, snps)

  /** Process top associations for each phenotype. */
  override val rules: PartialFunction[Input, Outputs] = {
    case transEthnic(phenotype) => Outputs.Named(phenotype)
    case ancestrySpecific(phenotype, ancestry) =>
      Outputs.Named(s"$phenotype/${ancestry.split("ancestry=").last}")
    case snps() => Outputs.All
  }

  /** Simple cluster with more memory. */
  override val cluster: ClusterDef = super.cluster.copy(
    masterInstanceType = Strategy.generalPurpose(mem = 64.gb),
    instances = 1,
    masterVolumeSizeInGB = 100,
    bootstrapSteps = Seq(Job.Script(resourceUri("install-plink.sh")))
  )

  /** Build the job. */
  override def make(output: String): Job = {
    // run clumping and then join with bottom line
    val flags = output.split("/").toSeq match {
      case Seq(phenotype) => Seq(s"--phenotype=$phenotype", s"--ancestry=Mixed")
      case Seq(phenotype, ancestry) => Seq(s"--phenotype=$phenotype", s"--ancestry=$ancestry")
    }

    val steps = Seq(
      Job.Script(resourceUri("runPlink.py"), flags:_*),
      Job.PySpark(resourceUri("clumpedAssociations.py"), flags:_*)
    )
    new Job(steps)
  }

  /** Nuke the staging directories before the job runs.
    */
  override def prepareJob(output: String): Unit = {
    output.split("/").toSeq match {
      case Seq(phenotype) =>
        context.s3.rm(s"out/metaanalysis/staging/clumped/$phenotype/")
        context.s3.rm(s"out/metaanalysis/staging/plink/$phenotype/")
      case Seq(phenotype, ancestry) =>
        context.s3.rm(s"out/metaanalysis/staging/ancestry-clumped/$phenotype/ancestry=$ancestry")
        context.s3.rm(s"out/metaanalysis/staging/ancestry-plink/$phenotype/ancestry=$ancestry")
    }

  }
}
