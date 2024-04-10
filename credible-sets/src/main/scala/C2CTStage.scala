package org.broadinstitute.dig.aggregator.methods.crediblesets

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

class C2CTStage(implicit context: Context) extends Stage {

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    masterVolumeSizeInGB = 100,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("download-cmdga.sh"))),
    stepConcurrency = 5
  )

  val credibleSets: Input.Source = Input.Source.Success("out/credible_sets/merged/*/*/")

  override val sources: Seq[Input.Source] = Seq(credibleSets)

  override val rules: PartialFunction[Input, Outputs] = {
    case credibleSets(phenotype, ancestry) => Outputs.Named(s"$phenotype/$ancestry")
  }

  override def make(output: String): Job = {
    val flags = output.split("/").toSeq match {
      case Seq(phenotype, ancestry) =>
        Seq(s"--phenotype=$phenotype", s"--ancestry=$ancestry")
    }

    new Job(Seq(Job.Script(resourceUri("C2CT.py"), flags:_*)))
  }
}
