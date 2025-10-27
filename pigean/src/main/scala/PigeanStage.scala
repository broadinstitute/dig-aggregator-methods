package org.broadinstitute.dig.aggregator.methods.pigean

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy

class PigeanStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val geneSetSizes = Seq("full_production")

  override val cluster: ClusterDef = super.cluster.copy(
    masterInstanceType = Strategy.computeOptimized(vCPUs = 16, mem = 32.gb),
    instances = 1,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("pigean-bootstrap.sh"))),
    stepConcurrency = 10
  )

  val inputs: Input.Source = Input.Source.Success("out/pigean/inputs/*/*/*/")

  override val sources: Seq[Input.Source] = Seq(inputs)

  override val rules: PartialFunction[Input, Outputs] = {
    case inputs(traitType, traitGroup, phenotype) => Outputs.Named(geneSetSizes.map { geneSetSize =>
        s"$traitType/$traitGroup/$phenotype/$geneSetSize"
      }: _*)
  }

  override def make(output: String): Job = {
    val flags: Seq[String] = output.split("/").toSeq match {
      case Seq(traitType, traitGroup, phenotype, geneSetSize) =>
        Seq(
          s"--trait-type=$traitType",
          s"--trait-group=$traitGroup",
          s"--phenotype=$phenotype",
          s"--gene-set-size=$geneSetSize")
    }
    new Job(Job.Script(resourceUri("runPigean.py"), flags:_*))
  }
}
