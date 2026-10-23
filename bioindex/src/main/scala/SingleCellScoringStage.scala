package org.broadinstitute.dig.aggregator.methods.bioindex

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

class SingleCellScoringStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val portal: Input.Source = Input.Source.Raw("out/single_cell/portal/*/*/portal.zip")

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("cluster-bootstrap.sh")))
  )

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(portal)

  /** Rules for mapping input to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case portal(_, _) => Outputs.Named("portal")
  }

  /** Output to Job steps. */
  override def make(dataset: String): Job = {
    new Job(Job.PySpark(resourceUri("singleCellScoring.py")))
  }
}
