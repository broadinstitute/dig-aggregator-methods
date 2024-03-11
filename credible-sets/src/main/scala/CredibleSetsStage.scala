package org.broadinstitute.dig.aggregator.methods.crediblesets

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

class CredibleSetsStage(implicit context: Context) extends Stage {

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("bootstrap.sh"))),
  )

  val credibleSets: Input.Source = Input.Source.Dataset("credible_sets/*/*/")
  val ancestryBottomLine: Input.Source = Input.Source.Success("out/metaanalysis/bottom-line/ancestry-clumped/*/*/")

  override val sources: Seq[Input.Source] = Seq(credibleSets, ancestryBottomLine)

  override val rules: PartialFunction[Input, Outputs] = {
    case credibleSets(dataset, phenotype) => Outputs.Named(s"credible-sets/$dataset/$phenotype")
    case ancestryBottomLine(phenotype, ancestry) => Outputs.Named(s"bottom-line/$phenotype/${ancestry.split("=").last}")
  }

  override def make(output: String): Job = {
    val flags = output.split("/").toSeq match {
      case Seq("credible-sets", dataset, phenotype) =>
        Seq("--source=credible-set", s"--dataset=$dataset", s"--phenotype=$phenotype")
      case Seq("bottom-line", phenotype, ancestry) =>
        Seq("--source=bottom-line", s"--phenotype=$phenotype", s"--ancestry=$ancestry")
    }

    new Job(Seq(Job.PySpark(resourceUri("credibleSets.py"), flags:_*)))
  }
}
