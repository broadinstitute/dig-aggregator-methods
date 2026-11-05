package org.broadinstitute.dig.aggregator.methods.ldsc

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy

class TranslateHeritabilityStage(implicit context: Context) extends Stage {

  val heritability: Input.Source = Input.Source.Success("out/ldsc/staging/heritability/*/")

  /** Source inputs. */
  override val sources: Seq[Input.Source] = Seq(heritability)

  /** Map inputs to their outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case heritability(phenotype) => Outputs.Named(phenotype)
  }

  /** Just need a single machine with no applications, but a good drive. */
  override def cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    applications = Seq.empty,
    bootstrapScripts = Seq(
      new BootstrapScript(resourceUri("install-translate.sh"))
    ),
    stepConcurrency = 5
  )

  override def make(output: String): Job = {
    new Job(Job.Script(resourceUri("translateHeritability.py"), s"--phenotype=$output"))
  }
}
