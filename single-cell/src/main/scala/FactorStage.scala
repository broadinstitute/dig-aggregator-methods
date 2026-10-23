package org.broadinstitute.dig.aggregator.methods.singlecell

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

class FactorStage(implicit context: Context) extends Stage {

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1
  )

  val factors: Input.Source = Input.Source.Raw("out/single_cell/staging/betas_phewas/*/*/*/programs/combined_pigean.tsv.gz")

  override val sources: Seq[Input.Source] = Seq(factors)

  override val rules: PartialFunction[Input, Outputs] = {
    case factors(tissue, cellType, dataset) => Outputs.Named(s"$tissue/$cellType/$dataset")
  }

  override def make(output: String): Job = {
    val flags: Seq[String] = output.split("/").toSeq match {
      case Seq(tissue, cellType, dataset) =>
        Seq(
          s"--tissue=$tissue",
          s"--cell-type=$cellType",
          s"--dataset=$dataset")
    }
    new Job(Job.Script(resourceUri("runFactors.py"), flags:_*))
  }
}
