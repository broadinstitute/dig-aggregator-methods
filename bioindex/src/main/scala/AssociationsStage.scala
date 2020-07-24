package org.broadinstitute.dig.aggregator.methods.bioindex

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

/** The final result of all aggregator methods is building the Bio Index. All
  * outputs are to the dig-bio-index bucket in S3.
  */
class AssociationsStage(implicit context: Context) extends Stage {
  val bottomLine = Input.Source.Success("out/metaanalysis/trans-ethnic/*/")
  val top        = Input.Source.Success("out/metaanalysis/top/*/")

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(bottomLine, top)

  /** Rules for mapping input to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case bottomLine(phenotype) => Outputs.Named(phenotype)
    case top(phenotype)        => Outputs.Named("top-associations")
  }

  /** Output to Job steps. */
  override def make(output: String): Job = {
    val global = resourceUri("associations/global.py")
    val matrix = resourceUri("associations/matrix.py")
    val top    = resourceUri("associations/top.py")

    // build top associations or global for a phenotype
    output match {
      case "top-associations" => new Job(Job.PySpark(top))
      case phenotype          => new Job(Job.PySpark(global, phenotype))
    }
  }
}
