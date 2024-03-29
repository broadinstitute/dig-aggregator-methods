package org.broadinstitute.dig.aggregator.methods.bottomline

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy

class MinPTransEthnicStage(implicit context: Context) extends Stage {

  val minP: Input.Source = Input.Source.Success("out/metaanalysis/min_p/ancestry-specific/*/*/")

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 6
  )

  override val sources: Seq[Input.Source] = Seq(minP)

  override val rules: PartialFunction[Input, Outputs] = {
    case minP(phenotype: String, _) => Outputs.Named(phenotype)
  }

  override def make(output: String): Job = {
    val minP = resourceUri("runMinPTransEthnic.py")

    new Job(Seq(Job.PySpark(minP, output)))
  }
}
