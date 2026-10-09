package org.broadinstitute.dig.aggregator.methods.falcon

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy

class PigeanStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val models = Seq("mouse_msigdb")

  override val cluster: ClusterDef = super.cluster.copy(
    masterInstanceType = Strategy.memoryOptimized(mem = 64.gb),
    instances = 1,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("bootstrap_pigean.sh"))),
    stepConcurrency = 1
  )

  val falcon: Input.Source = Input.Source.Raw("out/falcon/staging/falcon/*/*/gwas/*.gwas.tsv.gz")

  override val sources: Seq[Input.Source] = Seq(falcon)

  override val rules: PartialFunction[Input, Outputs] = {
    case falcon(traitGroup, phenotype, _) => Outputs.Named(models.map { model =>
      s"$traitGroup/$phenotype/$model"
    }: _*)
  }

  override def make(output: String): Job = {
    val flags: Seq[String] = output.split("/").toSeq match {
      case Seq(traitGroup, phenotype, model) =>
        Seq(
          s"--trait-group=$traitGroup",
          s"--phenotype=$phenotype",
          s"--model=$model")
    }
    new Job(Job.Script(resourceUri("runPigean.py"), flags:_*))
  }
}
