package org.broadinstitute.dig.aggregator.methods.falcon

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy

class FalconStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val inputs: Input.Source = Input.Source.Success("out/falcon/inputs/*/*/*/")
  /** Source inputs. */
  override val sources: Seq[Input.Source] = Seq(inputs)

  /** Map inputs to their outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case inputs(traitType, traitGroup, phenotype) => Outputs.Named(s"$traitType/$traitGroup/$phenotype")
  }

  /** Just need a single machine with no applications, but a good drive. */
  override def cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    applications = Seq.empty,
    masterVolumeSizeInGB = 100,
    masterInstanceType = Strategy.memoryOptimized(mem = 128.gb),
    bootstrapScripts = Seq(
      new BootstrapScript(resourceUri("bootstrap_falcon.sh")),
      new BootstrapScript(resourceUri("bootstrap_pigean.sh"))
    )
  )

  override def make(output: String): Job = {
    val flags: Seq[String] = output.split("/").toSeq match {
      case Seq(traitType, traitGroup, phenotype) =>
        Seq(
          s"--trait-type=$traitType",
          s"--trait-group=$traitGroup",
          s"--phenotype=$phenotype")
    }
    new Job(Job.Script(resourceUri("falcon.py"), flags:_*))
  }
}
