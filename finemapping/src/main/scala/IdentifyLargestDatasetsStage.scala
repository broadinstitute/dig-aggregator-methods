package org.broadinstitute.dig.aggregator.methods.finemapping

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy

/** final stage to collate all phenotype/ancestry results into single directory
  */
class IdentifyLargestDatasetsStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val cojo: Input.Source = Input.Source.Success("out/finemapping/cojo-results/*/")

  /** The output of gene pvalue analysis is the input for top results file. */
  override val sources: Seq[Input.Source] = Seq(cojo)

  /** Process top associations for each phenotype. */
  override val rules: PartialFunction[Input, Outputs] = {
    case cojo(_) => Outputs.Named("FinalResults")
  }

  /** Simple cluster with more memory. */
  override val cluster: ClusterDef = super.cluster.copy()

  /** Build the job. */
  override def make(output: String): Job = {
    val script    = resourceUri("identifyLargestDatasets.py")

    new Job(Job.PySpark(script))
  }
}
