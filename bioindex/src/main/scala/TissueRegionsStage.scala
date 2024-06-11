package org.broadinstitute.dig.aggregator.methods.bioindex

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._


class TissueRegionsStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val regions: Input.Source = Input.Source.Success(s"out/ldsc/regions/merged/annotation-tissue-biosample/")

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(regions)

  /** Rules for mapping input to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case _ => Outputs.Named("biosamples")
  }

  /** Output to Job steps. */
  override def make(output: String): Job = {
    new Job(Job.PySpark(resourceUri("tissueRegions.py")))
  }
}
