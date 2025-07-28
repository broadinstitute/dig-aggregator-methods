package org.broadinstitute.dig.aggregator.methods.ldsc

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy
import org.broadinstitute.dig.aws.MemorySize

class MakeServerSumstatsStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val ancestrySpecific: Input.Source = Input.Source.Success("out/metaanalysis/bottom-line/ancestry-specific/*/*/")
  val mixedDatasets: Input.Source = Input.Source.Success("variants/*/*/*/")

  /** Source inputs. */
  override val sources: Seq[Input.Source] = Seq(ancestrySpecific, mixedDatasets)

  /** Map inputs to their outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case ancestrySpecific(phenotype, ancestry) => Outputs.Named(s"$phenotype/${ancestry.split('=').last}")
    case mixedDatasets(_, _, phenotype) => Outputs.Named(s"$phenotype/Mixed")
  }

  /** Just need a single machine with no applications, but a good drive. */
  override def cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    applications = Seq.empty,
    masterVolumeSizeInGB = 100,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("install-server-sumstats.sh"))),
    stepConcurrency = 8
  )

  override def make(output: String): Job = {
    val input = MakeServerSumstatsInput.fromString(output)
    new Job(Job.Script(resourceUri("makeServerSumstats.py"), input.flags:_*))
  }

  /** Before the jobs actually run, perform this operation.
   */
  override def prepareJob(output: String): Unit = {
    val input = MakeServerSumstatsInput.fromString(output)
    context.s3.rm(input.outputDirectory + "/")
  }

  /** On success, write the _SUCCESS file in the output directory.
   */
  override def success(output: String): Unit = {
    val input = MakeServerSumstatsInput.fromString(output)
    context.s3.touch(input.outputDirectory + "/_SUCCESS")
    ()
  }
}

case class MakeServerSumstatsInput(
  phenotype: String,
  ancestry: String
) {
  def outputDirectory: String = s"out/ldsc/server_sumstats/$phenotype/ancestry=$ancestry"

  def flags: Seq[String] = Seq(s"--phenotype=$phenotype", s"--ancestry=$ancestry")
}

object MakeServerSumstatsInput {
  def fromString(output: String): MakeServerSumstatsInput = {
    output.split("/").toSeq match {
      case Seq(phenotype, ancestry) => MakeServerSumstatsInput(phenotype, ancestry)
    }
  }
}

