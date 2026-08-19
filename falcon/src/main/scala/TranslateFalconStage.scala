package org.broadinstitute.dig.aggregator.methods.falcon

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

class TranslateFalconStage(implicit context: Context) extends Stage {

  val falconGenes: Input.Source = Input.Source.Raw("out/falcon/staging/falcon/*/falcon.*.genes")

  /** Source inputs. */
  override val sources: Seq[Input.Source] = Seq(falconGenes)

  /** Map inputs to their outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case falconGenes(phenotype, _) => Outputs.Named(phenotype)
  }

  /** Just need a single machine with no applications, but a good drive. */
  override def cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    applications = Seq.empty
  )

  override def make(output: String): Job = {
    new Job(Job.Script(resourceUri("translateFalcon.py"), s"--trait=$output"))
  }
}
