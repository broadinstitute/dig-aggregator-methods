package org.broadinstitute.dig.aggregator.methods.pigean

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy

class PigeanStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  override val cluster: ClusterDef = super.cluster.copy(
    masterInstanceType = Strategy.computeOptimized(vCPUs = 16, mem = 32.gb),
    instances = 1,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("pigean-bootstrap.sh"))),
    stepConcurrency = 10
  )

  val sumstats: Input.Source = Input.Source.Success("out/pigean/sumstats/*/")

  override val sources: Seq[Input.Source] = Seq(sumstats)

  override val rules: PartialFunction[Input, Outputs] = {
    case sumstats(phenotype) => Outputs.Named(phenotype)
  }

  override def make(output: String): Job = {
    new Job(Job.Script(resourceUri("runPigean.py"), s"--phenotype=$output"))
  }
}
