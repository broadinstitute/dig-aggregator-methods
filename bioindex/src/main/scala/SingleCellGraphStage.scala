package org.broadinstitute.dig.aggregator.methods.bioindex

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

/** The final result of all aggregator methods is building the BioIndex. All
 * outputs are to the dig-bio-index bucket in S3.
 */
class SingleCellGraphStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val edges: Input.Source = Input.Source.Raw("out/single_cell/graph/*/*/*/*.json.gz")

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 3,
    masterVolumeSizeInGB = 100,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("cluster-bootstrap.sh")))
  )

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(edges)

  /** Rules for mapping input to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case edges(_, _, _, _) => Outputs.Named("graph")
  }

  /** Output to Job steps. */
  override def make(dataset: String): Job = {
    new Job(Job.PySpark(resourceUri("singleCellGraph.py")))
  }
}
