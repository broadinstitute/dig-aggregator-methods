package org.broadinstitute.dig.aggregator.methods.bioindex

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

/** The final result of all aggregator methods is building the BioIndex. All
 * outputs are to the dig-bio-index bucket in S3.
 */
class BassetStage(implicit context: Context) extends Stage {
  val basset = Input.Source.Success("out/basset/translated/")

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(basset)

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 5,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("cluster-bootstrap-6.7.0.sh"))),
    releaseLabel = ReleaseLabel("emr-6.7.0") // Need emr 6.1+ to read zstd files
  )

  /** Rules for mapping input to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case basset() => Outputs.Named("basset")
  }

  /** Output to Job steps. */
  override def make(output: String): Job = {
    new Job(Job.PySpark(resourceUri("basset.py")))
  }
}
