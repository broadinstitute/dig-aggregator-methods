package org.broadinstitute.dig.aggregator.methods.bioindex

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

class SingleCellMetadataStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val metadata: Input.Source = Input.Source.Raw("single_cell/*/dataset_metadata")

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    applications = Seq.empty,
  )

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(metadata)

  /** Rules for mapping input to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case metadata(_) => Outputs.Named("metadata")
  }

  /** Output to Job steps. */
  override def make(dataset: String): Job = {
    new Job(Job.Script(resourceUri("singleCellMetadata.py")))
  }
}
