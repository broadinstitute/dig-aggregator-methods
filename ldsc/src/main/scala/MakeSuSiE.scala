package org.broadinstitute.dig.aggregator.methods.susie

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy
import org.broadinstitute.dig.aws.MemorySize

class MakeSuSiE(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val ancestrySpecific: Input.Source = Input.Source.Success("out/metaanalysis/bottom-line/ancestry-clumped/*/*/")
  // val mixedDatasets: Input.Source = Input.Source.Success("variants/*/*/*/")

  /** Source inputs. */
  override val sources: Seq[Input.Source] = Seq(ancestrySpecific, mixedDatasets)

  /** Map inputs to their outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case ancestrySpecific(phenotype, ancestry) => Outputs.Named(s"$phenotype/${ancestry.split('=').last}")
    // case mixedDatasets(_, _, phenotype) => Outputs.Named(s"$phenotype/Mixed")
  }

  /** Just need a single machine with no applications, but a good drive. */
  override def cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    applications = Seq.empty,
    masterVolumeSizeInGB = 100,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("install-susie.sh")))
  )

  override def make(output: String): Job = {
    val input = MakeSuSiEInput.fromString(output)
    new Job(Job.Script(resourceUri("makeSuSiE.py"), input.flags:_*))
  }

  /** Before the jobs actually run, perform this operation.
    */
  override def prepareJob(output: String): Unit = {
    val input = MakeSuSiEInput.fromString(output)
    context.s3.rm(input.outputDirectory + "/")
  }

  /** On success, write the _SUCCESS file in the output directory.
    */
  override def success(output: String): Unit = {
    val input = MakeSuSiEInput.fromString(output)
    context.s3.touch(input.outputDirectory + "/_SUCCESS")
    ()
  }
}

case class MakeSuSiEInput(
  clump: String,
  varId2rsId: String,
  ld-folder: String,
  out-folder: String
) {
  def outputDirectory: String = s"out/susie/$phenotype/ancestry=$ancestry"

  def flags: Seq[String] = Seq(s"--clump=$MakeSuSiE.ancestrySpecific($phenotype, $ancestry)", 
                               s"--varId2rsId=$ancestry",
                               s"--ld-folder=$ancestry",
                               s"--out-folder=$outputDirectory")
}

object MakeSumstatsInput {
  def fromString(output: String): MakeSumstatsInput = {
    output.split("/").toSeq match {
      case Seq(phenotype, ancestry) => MakeSumstatsInput(phenotype, ancestry)
    }
  }
}

