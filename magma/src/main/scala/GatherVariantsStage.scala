package org.broadinstitute.dig.aggregator.methods.magma

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._


class GatherVariantsStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val variants: Input.Source = Input.Source.Success("out/varianteffect/variants/variants/")

  /** Source inputs. */
  override val sources: Seq[Input.Source] = Seq(variants)

  /* Define settings for the cluster to run the job.
   */
  override val cluster: ClusterDef = super.cluster.copy()

  /** Map inputs to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case _ => Outputs.Named("GatherVariants")
  }

  /** All that matters is that there are new datasets. The input datasets are
    * actually ignored, and _everything_ is reprocessed. This is done because
    * there is only a single analysis node for all variants.
    */
  override def make(output: String): Job = {
    new Job(Job.PySpark(resourceUri("gatherVariants.py")))
  }
}
