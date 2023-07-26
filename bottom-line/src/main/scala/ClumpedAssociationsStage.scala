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
  val datasets = Input.Source.Dataset("variants/*/*/*/")
  val snps: Input.Source       = Input.Source.Success("out/varianteffect/snp/")

  /** The output of meta-analysis is the input for top associations. */
  override val sources: Seq[Input.Source] = Seq(transEthnic, ancestrySpecific, datasets, snps)

  /** Process top associations for each phenotype. */
  override val rules: PartialFunction[Input, Outputs] = {
    case transEthnic(phenotype) => Outputs.Named(s"trans-ethnic/$phenotype")
    case ancestrySpecific(phenotype, ancestry) => Outputs.Named(s"ancestry/$phenotype/$ancestry")
    case datasets(tech, dataset, phenotype) if tech == "GWAS" => Outputs.Named(s"dataset/$phenotype/$dataset")
    case snps() => Outputs.All
  }

  /** Simple cluster with more memory. */
  override val cluster: ClusterDef = super.cluster.copy(
    masterInstanceType = Strategy.generalPurpose(mem = 128.gb),
    instances = 1,
    masterVolumeSizeInGB = 100,
    bootstrapSteps = Seq(Job.Script(resourceUri("install-plink.sh"))),
    releaseLabel = ReleaseLabel("emr-6.7.0") // Need emr 6.1+ to read zstd files
  )

  /** Build the job. */
  override def make(output: String): Job = {
    val input = ClumpedAssociationsInput.fromOutput(output)

    val steps = Seq(
      Job.Script(resourceUri("runPlink.py"), input.flags:_*),
      Job.PySpark(resourceUri("clumpedAssociations.py"), input.flags:_*)
    )
    new Job(steps)
  }

  /** Nuke the staging directories before the job runs.
    */
  override def prepareJob(output: String): Unit = {
    val input = ClumpedAssociationsInput.fromOutput(output)
    input.outputDirectory("clumped").foreach { clumpingDirectory =>
      context.s3.rm(clumpingDirectory)
    }
    input.outputDirectory("plink").foreach { plinkDirectory =>
      context.s3.rm(plinkDirectory)
    }
  }
}

case class ClumpedAssociationsInput(
  inputType: String,
  phenotype: String,
  maybeAncestry: Option[String],
  maybeDataset: Option[String]
) {
  def flags: Seq[String] = Seq(
    Some(s"--phenotype=$phenotype"),
    maybeAncestry.map(ancestry => s"--ancestry=$ancestry"),
    maybeDataset.map(dataset => s"--dataset=$dataset")
  ).flatten

  def outputDirectory(outputType: String): Option[String] = inputType match {
    case "trans-ethnic" => Some(s"out/metaanalysis/staging/$outputType/$phenotype/")
    case "ancestry" => maybeAncestry.map { ancestry =>
      s"out/metaanalysis/staging/ancestry-$outputType/$phenotype/ancestry=$ancestry"
    }
    case "dataset" => maybeDataset.map { dataset =>
      s"out/metaanalysis/staging/dataset-$outputType/$phenotype/dataset=$dataset"
    }
  }
}

case object ClumpedAssociationsInput {
  def fromOutput(output: String): ClumpedAssociationsInput = {
    output.split("/").toSeq match {
      case Seq(inputType, phenotype) if inputType == "trans-ethnic" =>
        ClumpedAssociationsInput(inputType, phenotype, Some("Mixed"), None)
      case Seq(inputType, phenotype, ancestry) if inputType == "ancestry" =>
        ClumpedAssociationsInput(inputType, phenotype, Some(ancestry.split("=").last), None)
      case Seq(inputType, phenotype, dataset) if inputType == "dataset" =>
        ClumpedAssociationsInput(inputType, phenotype, None, Some(dataset.split("=").last))
    }
  }
}
