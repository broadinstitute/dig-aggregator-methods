package org.broadinstitute.dig.aggregator.methods.bioindex

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

class SingleCellGeneStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val genes: Input.Source = Input.Source.Raw("single_cell_genes/all_sig_genes.all_datasets.tsv.gz")

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1
  )

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(genes)

  /** Rules for mapping input to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case genes() => Outputs.Named("genes")
  }

  /** Output to Job steps. */
  override def make(dataset: String): Job = {
    new Job(Job.PySpark(resourceUri("singleCellGene.py")))
  }
}
