package org.broadinstitute.dig.aggregator.methods.pigean

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy

class PigeanBetasStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val models = Seq("cfde-inc-v2")

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    masterInstanceType = Strategy.computeOptimized(vCPUs = 16, mem = 64.gb),
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("pigean-bootstrap.sh"))),
    stepConcurrency = 10
  )

  val small_model: Input.Source = Input.Source.Raw("out/pigean/staging/pigean/*/*/mouse_msigdb/gs.out")

  override val sources: Seq[Input.Source] = Seq(small_model)

  override val rules: PartialFunction[Input, Outputs] = {
    case small_model(traitGroup, phenotype) => Outputs.Named(models.map { model =>
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
    new Job(Job.Script(resourceUri("runPigeanBetas.py"), flags:_*))
  }
}
