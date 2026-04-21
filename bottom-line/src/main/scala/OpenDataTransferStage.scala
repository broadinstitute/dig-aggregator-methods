package org.broadinstitute.dig.aggregator.methods.bottomline

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy

class OpenDataTransferStage(implicit context: Context) extends Stage {

  override val cluster: ClusterDef = super.cluster.copy(
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("open_data_transfer_bootstrap.sh")))
  )

  val transEthnicInputs: Input.Source = Input.Source.Success("out/metaanalysis/bottom-line/trans-ethnic/*/")
  val ancestrySpecificInputs: Input.Source = Input.Source.Success("out/metaanalysis/bottom-line/ancestry-specific/*/*/")

  override val sources: Seq[Input.Source] = Seq(transEthnicInputs, ancestrySpecificInputs)


  override val rules: PartialFunction[Input, Outputs] = {
    case ancestrySpecificInputs(phenotype, ancestry) => Outputs.Named(s"$phenotype/${ancestry.split("ancestry=").last}")
    case transEthnicInputs(phenotype) => Outputs.Named(s"$phenotype/Mixed")
  }


  override def make(output: String): Job = {
    val flags = output.split("/").toSeq match {
      case Seq(phenotype, ancestry) =>
        Seq(s"--phenotype=$phenotype", s"--ancestry=$ancestry")
    }
    new Job(Seq(Job.PySpark(resourceUri("openDataTransfer.py"), flags:_*)))
  }
}
