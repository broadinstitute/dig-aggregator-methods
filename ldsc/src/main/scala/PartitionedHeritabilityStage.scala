package org.broadinstitute.dig.aggregator.methods.ldsc

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy
import org.broadinstitute.dig.aws.MemorySize

class PartitionedHeritabilityStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val sumstats: Input.Source = Input.Source.Success("out/ldsc/sumstats/*/*/")
  //TODO: Add in _SUCCESS files for this to work
  //val subRegion: String = if (partitions.isEmpty) "default" else partitions.mkString("-")
  //val annotations: Input.Source = Input.Source.Success(s"out/ldsc/region/$subRegion/ld_score/*/*/")

  /** Source inputs. */
  override val sources: Seq[Input.Source] = Seq(sumstats)

  /** Map inputs to their outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case sumstats(phenotype, ancestry) => Outputs.Named(s"$phenotype/${ancestry.split('=').last}")
  }

  /** Just need a single machine with no applications, but a good drive. */
  override def cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    applications = Seq.empty,
    bootstrapScripts = Seq(
      new BootstrapScript(resourceUri("install-ldscore.sh")),
      new BootstrapScript(resourceUri("download-annotation-files.sh"))
    ),
    masterInstanceType = Strategy.computeOptimized(vCPUs = 16, mem = 16.gb),
    releaseLabel = ReleaseLabel("emr-6.7.0")
  )

  override def make(output: String): Job = {
    val input = PartitionedHeritabilityInput.fromString(output)
    new Job(Job.Script(resourceUri("runPartitionedHeritability.py"), input.flags:_*))
  }

  /** Before the jobs actually run, perform this operation.
   */
  override def prepareJob(output: String): Unit = {
    val input = PartitionedHeritabilityInput.fromString(output)
    context.s3.rm(s"${input.outputDirectory}/")
  }

  /** On success, write the _SUCCESS file in the output directory.
   */
  override def success(output: String): Unit = {
    val input = PartitionedHeritabilityInput.fromString(output)
    context.s3.touch(s"${input.outputDirectory}/_SUCCESS")
    ()
  }
}

case class PartitionedHeritabilityInput(
  phenotype: String,
  ancestry: String
) {
  def outputDirectory: String = s"out/ldsc/staging/partitioned_heritability/$phenotype/ancestry=$ancestry"

  def flags: Seq[String] = Seq(s"--phenotype=$phenotype", s"--ancestry=$ancestry")
}

object PartitionedHeritabilityInput {
  def fromString(output: String): PartitionedHeritabilityInput = {
    output.split("/").toSeq match {
      case Seq(phenotype, ancestry) => PartitionedHeritabilityInput(phenotype, ancestry)
    }
  }
}
