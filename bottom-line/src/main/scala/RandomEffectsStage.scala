package org.broadinstitute.dig.aggregator.methods.bottomline

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy

class RandomEffectsStage(implicit context: Context) extends Stage {

  val ancestrySpecific: Input.Source = Input.Source.Success("out/metaanalysis/bottom-line/ancestry-specific/*/")

  // NOTE: If jobs report a mem_alloc issue bump the instance memory. For disk space errors increase the volume size
  override val cluster: ClusterDef = super.cluster.copy(
    masterInstanceType = Strategy.memoryOptimized(),
    instances = 1,
    applications = Seq.empty,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("cluster-bootstrap.sh")))
  )

  override val sources: Seq[Input.Source] = Seq(ancestrySpecific)

  override val rules: PartialFunction[Input, Outputs] = {
    case ancestrySpecific(phenotype) => Outputs.Named(phenotype)
  }

  override def make(output: String): Job = {
    val transEthnic = resourceUri("runTransEthnic.sh")

    new Job(Seq(Job.Script(transEthnic, output)))
  }
}
