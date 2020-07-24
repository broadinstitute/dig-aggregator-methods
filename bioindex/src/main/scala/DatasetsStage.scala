package org.broadinstitute.dig.aggregator.methods.bioindex

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

/** The final result of all aggregator methods is building the Bio Index. All
  * outputs are to the dig-bio-index bucket in S3.
  */
class DatasetsStage(implicit context: Context) extends Stage {
  val variants = Input.Source.Dataset("variants/*/*/")

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(variants)

  /** Rules for mapping input to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case variants(_, phenotype) => Outputs.Named("allDatasets", phenotype)
  }

  /** Output to Job steps. */
  override def make(output: String): Job = {
    val datasetsScript       = resourceUri("datasets/global.py")
    val varianDatasetsScript = resourceUri("datasets/variants.py")

    output match {
      case "allDatasets" => new Job(Job.PySpark(datasetsScript))
      case phenotype     => new Job(Job.PySpark(varianDatasetsScript, phenotype))
    }
  }
}
