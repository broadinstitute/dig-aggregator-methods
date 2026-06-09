package org.broadinstitute.dig.aggregator.methods.singlecell

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

class GraphStage(implicit context: Context) extends Stage {

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    masterVolumeSizeInGB = 500,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("bootstrap-downsample.sh")))
  )

  val factorMatrix: Input.Source = Input.Source.Raw("out/single_cell/staging/factor_matrix/*/*/*/factor_matrix_factors.tsv")

  override val sources: Seq[Input.Source] = Seq(factorMatrix)

  override val rules: PartialFunction[Input, Outputs] = {
    case factorMatrix(_, _, _) => Outputs.Named("graph")
  }

  override def make(output: String): Job = {
    new Job(Job.Script(resourceUri("graph.py")))
  }
}
