package org.broadinstitute.dig.aggregator.methods.bioindex

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.config.emr._
import org.broadinstitute.dig.aws.emr._

/** The final result of all aggregator methods is building the BioIndex. All
 * outputs are to the dig-bio-index bucket in S3.
 */
class KidsFirstDiffExpStage(implicit context: Context) extends Stage {

  override val cluster: ClusterDef = super.cluster.copy(
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("kidsfirst-bootstrap.sh")))
  )

  val diffExp = Input.Source.Raw("tissue-sumstats/*")

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(diffExp)

  /** Rules for mapping input to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case diffExp(_) => Outputs.Named("diffExp")
  }

  /** Output to Job steps. */
  override def make(output: String): Job = {
    new Job(Job.PySpark(resourceUri("KidsFirstDiffExp.py")))
  }
}
