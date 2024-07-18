package org.broadinstitute.dig.aggregator.methods.bottomline

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy


class ClumpedMergeStage(implicit context: Context) extends Stage {

  val transEthnic: Input.Source = Input.Source.Raw("out/metaanalysis/*/staging/clumped/analysis/*/variants.json")
  val ancestrySpecific: Input.Source = Input.Source.Raw("out/metaanalysis/*/staging/ancestry-clumped/analysis/*/*/variants.json")

  override val sources: Seq[Input.Source] = Seq(transEthnic, ancestrySpecific)

  override val rules: PartialFunction[Input, Outputs] = {
    case transEthnic(_, phenotype) => Outputs.Named(s"$phenotype/TE")
    case ancestrySpecific(_, phenotype, ancestry) => Outputs.Named(s"$phenotype/${ancestry.split("ancestry=").last}")
  }

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    stepConcurrency = 5
  )

  override def make(output: String): Job = {
    val flags = output.split("/").toSeq match {
      case Seq(phenotype, ancestry) =>
        Seq(s"--phenotype=$phenotype", s"--ancestry=$ancestry")
    }

    val steps = Seq(
      Job.Script(resourceUri("mergeClumps.py"), flags:_*)
    )
    new Job(steps)
  }
}
