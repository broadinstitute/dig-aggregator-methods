package org.broadinstitute.dig.aggregator.methods.bioindex

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

class PigeanStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val gene: Input.Source = Input.Source.Success("out/old-pigean/combined_gene_stats/*/")
  val geneset: Input.Source = Input.Source.Success("out/old-pigean/combined_gene_set_stats/*/")
  val genegeneset: Input.Source = Input.Source.Success("out/old-pigean/gene_gene_set_stats/*/")

  override val sources: Seq[Input.Source] = Seq(gene, geneset, genegeneset)

  override val rules: PartialFunction[Input, Outputs] = {
    case gene(_) => Outputs.Named("pigean")
    case geneset(_) => Outputs.Named("pigean")
    case genegeneset(_) => Outputs.Named("pigean")
  }

  override val cluster: ClusterDef = super.cluster.copy(
    masterVolumeSizeInGB = 100,
    slaveVolumeSizeInGB = 100,
    instances = 10
  )

  /** Output to Job steps. */
  override def make(output: String): Job = {
    new Job(Job.PySpark(resourceUri("pigean.py")))
  }
}
