package org.broadinstitute.dig.aggregator.methods.pigean

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

class TranslatePigeanStage(implicit context: Context) extends Stage {

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1
  )

  val pigean: Input.Source = Input.Source.Success("out/pigean/staging/pigean/*/")

  override val sources: Seq[Input.Source] = Seq(pigean)

  override val rules: PartialFunction[Input, Outputs] = {
    case pigean(phenotype) => Outputs.Named(phenotype)
  }

  override def make(output: String): Job = {
    new Job(Job.Script(resourceUri("translatePigean.py"), s"--phenotype=$output"))
  }
}
