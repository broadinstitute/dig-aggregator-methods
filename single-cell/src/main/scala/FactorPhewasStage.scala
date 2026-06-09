package org.broadinstitute.dig.aggregator.methods.singlecell

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy

class FactorPhewasStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    masterInstanceType = Strategy.generalPurpose(mem = 64.gb),
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("bootstrap-factor.sh")))
  )

  val factorMatrix: Input.Source = Input.Source.Raw("out/single_cell/staging/factor_matrix/*/*/*/factor_matrix_gene_loadings.tsv")

  override val sources: Seq[Input.Source] = Seq(factorMatrix)

  override val rules: PartialFunction[Input, Outputs] = {
    case factorMatrix(dataset, cellType, model) => Outputs.Named(s"$dataset/$cellType/$model")
  }

  override def make(output: String): Job = {
    val flags: Seq[String] = output.split("/").toSeq match {
      case Seq(dataset, cellType, model) =>
        Seq(
          s"--dataset=$dataset",
          s"--cell-type=$cellType",
          s"--model=$model")
    }
    new Job(Job.Script(resourceUri("factorPhewas.py"), flags:_*))
  }
}
