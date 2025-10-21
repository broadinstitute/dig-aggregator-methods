package org.broadinstitute.dig.aggregator.methods.pigean

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

class TranslatePigeanStage(implicit context: Context) extends Stage {

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    stepConcurrency = 5
  )

  val pigean: Input.Source = Input.Source.Success("out/pigean/staging/pigean/*/*/*/")

  override val sources: Seq[Input.Source] = Seq(pigean)

  override val rules: PartialFunction[Input, Outputs] = {
    case pigean(traitGroup, phenotype, geneSetSize) =>
      Outputs.Named(s"$traitGroup/$phenotype/$geneSetSize")
  }

  override def make(output: String): Job = {
    val flags: Seq[String] = output.split("/").toSeq match {
      case Seq(traitGroup, phenotype, geneSetSize) =>
        Seq(s"--trait-group=$traitGroup", s"--phenotype=$phenotype", s"--gene-set-size=$geneSetSize")
      case _ => throw new Exception("output must take form <trait group>/<phenotype>/<gene set size>")
    }

    new Job(Job.Script(resourceUri("translatePigean.py"), flags:_*))
  }
}
