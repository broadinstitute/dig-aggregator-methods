package org.broadinstitute.dig.aggregator.methods.bioindex

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

/** The final result of all aggregator methods is building the BioIndex. All
 * outputs are to the dig-bio-index bucket in S3.
 */
class SingleCellBulkStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val geneNormCount: Input.Source = Input.Source.Raw("bulk_rna/*/norm_counts.tsv.gz")

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("cluster-bootstrap.sh")))
  )

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(geneNormCount)

  /** Rules for mapping input to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case geneNormCount(dataset) => Outputs.Named(dataset)
  }

  /** Output to Job steps. */
  override def make(dataset: String): Job = {
    new Job(Job.Script(resourceUri("singleCellBulk.py"), s"--dataset=$dataset"))
  }
}
