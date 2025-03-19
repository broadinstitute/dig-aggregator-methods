package org.broadinstitute.dig.aggregator.methods.bottomline

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy

class TranslateRandomEffectsStage(implicit context: Context) extends Stage {

  val randomEffects: Input.Source = Input.Source.Success("out/metaanalysis/bottom-line/staging/random-effects/*/")

  // NOTE: If jobs report a mem_alloc issue bump the instance memory. For disk space errors increase the volume size
  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    applications = Seq.empty,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("metasoft-bootstrap.sh")))
  )

  override val sources: Seq[Input.Source] = Seq(randomEffects)

  override val rules: PartialFunction[Input, Outputs] = {
    case randomEffects(phenotype) => Outputs.Named(phenotype)
  }

  override def make(output: String): Job = {
    new Job(Seq(Job.Script(resourceUri("translateRandomEffects.py"), s"--phenotype=$output")))
  }
}
