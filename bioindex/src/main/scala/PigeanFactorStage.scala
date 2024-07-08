package org.broadinstitute.dig.aggregator.methods.bioindex

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

class PigeanFactorStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val factor: Input.Source = Input.Source.Success("out/pigean/factor/*/")
  val geneFactor: Input.Source = Input.Source.Success("out/pigean/gene_factor/*/")
  val genesetFactor: Input.Source = Input.Source.Success("out/pigean/gene_set_factor/*/")

  override val sources: Seq[Input.Source] = Seq(factor, geneFactor, genesetFactor)

  override val rules: PartialFunction[Input, Outputs] = {
    case factor(_) => Outputs.Named("pigeanFactor")
    case geneFactor(_) => Outputs.Named("pigeanFactor")
    case genesetFactor(_) => Outputs.Named("pigeanFactor")
  }

  override val cluster: ClusterDef = super.cluster.copy(
    masterVolumeSizeInGB = 100,
    slaveVolumeSizeInGB = 100,
    instances = 6
  )

  /** Output to Job steps. */
  override def make(output: String): Job = {
    new Job(Job.PySpark(resourceUri("pigeanFactor.py")))
  }
}
