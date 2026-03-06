package org.broadinstitute.dig.aggregator.methods.pigean

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy

class FactorPigeanStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    masterInstanceType = Strategy.memoryOptimized(vCPUs = 8, mem = 64.gb),
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("pigean-bootstrap.sh"))),
    stepConcurrency = 8
  )

  val pigean: Input.Source = Input.Source.Success("out/pigean/staging/pigean/*/*/*/")

  override val sources: Seq[Input.Source] = Seq(pigean)

  override val rules: PartialFunction[Input, Outputs] = {
    case pigean(traitGroup, phenotype, geneSetSize) =>
      val outputs: Seq[String] = Seq("3", "5").map { phi =>
        s"$traitGroup/$phenotype/$geneSetSize/$phi"
      }
      Outputs.Named(outputs: _*)
  }

  override def make(output: String): Job = {
    val flags: Seq[String] = output.split("/").toSeq match {
      case Seq(traitGroup, phenotype, geneSetSize, phi) =>
        Seq(s"--trait-group=$traitGroup", s"--phenotype=$phenotype", s"--gene-set-size=$geneSetSize", s"--phi=$phi")
    }
    new Job(Job.Script(resourceUri("factorPigean.py"), flags: _*))
  }
}
