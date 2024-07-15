package org.broadinstitute.dig.aggregator.methods.crediblesets

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

class SpecificityStage(implicit context: Context) extends Stage {

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1
  )

  val c2ct: Input.Source = Input.Source.Success("out/credible_sets/c2ct/*/*/")

  override val sources: Seq[Input.Source] = Seq(c2ct)

  override val rules: PartialFunction[Input, Outputs] = {
    case c2ct(phenotype, ancestry) => s"$phenotype/$ancestry"
  }

  override def make(output: String): Job = {
    val flags = output.split("/").toSeq match {
      case Seq(phenotype, ancestry) => Seq(s"--phenotype=$phenotype", s"--ancestry=$ancestry")
      case _ => throw new Exception(s"invalid Input $output, must be of the form <phenotype>/<ancestry>")
    }

    new Job(Seq(Job.Script(resourceUri("specificity.py"), flags:_*)))
  }
}
