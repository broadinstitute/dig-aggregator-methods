package org.broadinstitute.dig.aggregator.methods.vep

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._


class ListVariantsStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val datasetVariants: Input.Source = Input.Source.Success("variants/")
  val ldServerVariants: Input.Source = Input.Source.Success("ld_server/variants/*/")
  val variantCounts: Input.Source = Input.Source.Dataset("variant_counts/*/*/*/")

  /** Source inputs. */
  override val sources: Seq[Input.Source] = Seq(datasetVariants, ldServerVariants, variantCounts)

  /* Define settings for the cluster to run the job.
   */
  override val cluster: ClusterDef = super.cluster.copy(
    masterVolumeSizeInGB = 400,
    slaveVolumeSizeInGB = 400,
    instances = 8,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("list-variant-bootstrap.sh")))
  )

  /** Map inputs to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case datasetVariants() => Outputs.Named("variants")
    case ldServerVariants(_) => Outputs.Named("ld_server")
    case variantCounts(_, _, _) => Outputs.Named("variant_counts")
  }

  /** All that matters is that there are new datasets. The input datasets are
    * actually ignored, and _everything_ is reprocessed. This is done because
    * there is only a single analysis node for all variants.
    */
  override def make(output: String): Job = {
    new Job(Job.PySpark(resourceUri("listVariants.py"), s"--data-type=$output"))
  }
}
