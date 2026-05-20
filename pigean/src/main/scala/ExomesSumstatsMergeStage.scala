package org.broadinstitute.dig.aggregator.methods.pigean

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

class ExomesSumstatsMergeStage(implicit context: Context) extends Stage {

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("exomes-sumstats-merge-bootstrap.sh"))),
  )

  val exomes: Input.Source = Input.Source.Success("out/pigean/inputs/exomes/*/*/")

  override val sources: Seq[Input.Source] = Seq(exomes)

  override val rules: PartialFunction[Input, Outputs] = {
    case exomes(exomesGroup, exomesTrait) => Outputs.Named(s"$exomesGroup/$exomesTrait")
  }

  override def make(output: String): Job = {
    val flags: Seq[String] = output.split("/").toSeq match {
      case Seq(exomesGroup, exomesTrait) =>
        Seq(
          s"--exomes-group=$exomesGroup",
          s"--exomes-trait=$exomesTrait"
        )
    }
    new Job(Job.Script(resourceUri("exomesSumstatsMerge.py"), flags:_*))
  }
}
