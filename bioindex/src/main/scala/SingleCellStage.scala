package org.broadinstitute.dig.aggregator.methods.bioindex

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

/** The final result of all aggregator methods is building the BioIndex. All
 * outputs are to the dig-bio-index bucket in S3.
 */
class SingleCellStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val fields: Input.Source = Input.Source.Raw("single_cell/*/metadata.tsv")
  val coordinates: Input.Source = Input.Source.Raw("single_cell/*/coordinates.tsv")
  val gene: Input.Source = Input.Source.Raw("single_cell/*/raw_counts.tsv.gz")
  val geneLogNorm: Input.Source = Input.Source.Raw("single_cell/*/lognorm_counts.tsv.gz")

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    masterVolumeSizeInGB = 100
  )

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(fields, coordinates, gene, geneLogNorm)

  /** Rules for mapping input to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case fields(dataset) => Outputs.Named(dataset)
    case coordinates(dataset) => Outputs.Named(dataset)
    case gene(dataset) => Outputs.Named(dataset)
    case geneLogNorm(dataset) => Outputs.Named(dataset)
  }

  /** Output to Job steps. */
  override def make(dataset: String): Job = {
    new Job(Job.Script(resourceUri("singleCell.py"), s"--dataset=$dataset"))
  }
}
