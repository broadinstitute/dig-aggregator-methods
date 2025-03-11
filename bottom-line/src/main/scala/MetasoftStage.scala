package org.broadinstitute.dig.aggregator.methods.bottomline

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy

class MetasoftStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val ancestrySpecific: Input.Source = Input.Source.Success("out/metaanalysis/bottom-line/ancestry-specific/*/")

  // NOTE: If jobs report a mem_alloc issue bump the instance memory. For disk space errors increase the volume size
  override val cluster: ClusterDef = super.cluster.copy(
    masterInstanceType = Strategy.memoryOptimized(mem = 128.gb),
    instances = 1,
    applications = Seq.empty,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("metasoft-bootstrap.sh")))
  )

  override val sources: Seq[Input.Source] = Seq(ancestrySpecific)

  override val rules: PartialFunction[Input, Outputs] = {
    case ancestrySpecific(phenotype) => Outputs.Named(phenotype)
  }

  override def make(output: String): Job = {
    new Job(Seq(Job.Script(resourceUri("runMetasoft.py"), s"--phenotype=$output")))
  }
}
