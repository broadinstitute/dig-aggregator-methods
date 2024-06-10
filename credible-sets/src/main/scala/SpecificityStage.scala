package org.broadinstitute.dig.aggregator.methods.crediblesets

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

class SpecificityStage(implicit context: Context) extends Stage {

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1
  )

  val entropyTypes: Seq[String] = Seq("all")

  val c2ct: Input.Source = Input.Source.Success("out/credible_sets/c2ct/*/*/")

  override val sources: Seq[Input.Source] = Seq(c2ct)

  override val rules: PartialFunction[Input, Outputs] = {
    case c2ct(phenotype, ancestry) => Outputs.Named(entropyTypes.map {
      entropyType => s"$phenotype/$ancestry/$entropyType"
    }: _*)
  }

  override def make(output: String): Job = {
    val flags = output.split("/").toSeq match {
      case Seq(phenotype, ancestry, entropyType) =>
        Seq(s"--phenotype=$phenotype", s"--ancestry=$ancestry", s"--entropy-type=$entropyType")
      case _ => throw new Exception(s"invalid Input $output, must be of the form <phenotype>/<ancestry>/<entropyType>")
    }

    new Job(Seq(Job.Script(resourceUri("specificity.py"), flags:_*)))
  }
}
