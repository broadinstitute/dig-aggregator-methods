package org.broadinstitute.dig.aggregator.methods.bioindex

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

class PigeanPhenotypesStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val gene: Input.Source = Input.Source.Success("out/pigean/combined_gene_stats/*/")
  val geneset: Input.Source = Input.Source.Success("out/pigean/combined_gene_set_stats/*/")
  val genegeneset: Input.Source = Input.Source.Success("out/pigean/gene_gene_set_stats/*/")

  override val sources: Seq[Input.Source] = Seq(gene, geneset, genegeneset)

  override val rules: PartialFunction[Input, Outputs] = {
    case gene(_) => Outputs.Named("pigeanPhenotypes")
    case geneset(_) => Outputs.Named("pigeanPhenotypes")
    case genegeneset(_) => Outputs.Named("pigeanPhenotypes")
  }

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1
  )

  /** Output to Job steps. */
  override def make(output: String): Job = {
    new Job(Job.PySpark(resourceUri("pigeanPhenotypes.py")))
  }
}
