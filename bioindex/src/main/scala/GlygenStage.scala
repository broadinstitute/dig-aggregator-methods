package org.broadinstitute.dig.aggregator.methods.bioindex

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.config.emr._
import org.broadinstitute.dig.aws.emr._

class GlygenStage(implicit context: Context) extends Stage {
  val geneData: Input.Source = Input.Source.Raw("genes_data/glycosylated_genes.json")

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(geneData)

  /** Rules for mapping input to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case geneData() => Outputs.Named("glygen")
  }

  /** Output to Job steps. */
  override def make(output: String): Job = {
    new Job(Job.PySpark(resourceUri("glygen.py")))
  }
}
