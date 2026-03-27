package org.broadinstitute.dig.aggregator.methods.bioindex

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

class SingleCellGeneStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val genes: Input.Source = Input.Source.Raw("single_cell/*/marker_genes.tsv")
  val bulk: Input.Source = Input.Source.Raw("bulk_rna/*/dea.tsv.gz")

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("cluster-bootstrap.sh")))
  )

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(genes, bulk)

  /** Rules for mapping input to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case genes(_) => Outputs.Named("genes")
    case bulk(_) => Outputs.Named("genes")
  }

  /** Output to Job steps. */
  override def make(dataset: String): Job = {
    new Job(Job.PySpark(resourceUri("singleCellGene.py")))
  }
}
