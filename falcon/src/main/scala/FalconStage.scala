package org.broadinstitute.dig.aggregator.methods.falcon

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy

class FalconStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val euBottomLine: Input.Source = Input.Source.Success("out/metaanalysis/bottom-line/ancestry-specific/*/ancestry=EU/")

  /** Source inputs. */
  override val sources: Seq[Input.Source] = Seq(euBottomLine)

  /** Map inputs to their outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case euBottomLine(phenotype) => Outputs.Named(phenotype)
  }

  /** Just need a single machine with no applications, but a good drive. */
  override def cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    applications = Seq.empty,
    masterVolumeSizeInGB = 100,
    masterInstanceType = Strategy.computeOptimized(),
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("bootstrap_falcon.sh")))
  )

  override def make(output: String): Job = {
    new Job(Job.Script(resourceUri("falcon.py"), s"--phenotype=$output"))
  }
}
