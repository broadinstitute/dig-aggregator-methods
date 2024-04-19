package org.broadinstitute.dig.aggregator.methods.vep

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

/** Finds all the variants in a dataset across all phenotypes and writes them
  * out to a set of files that can have VEP run over in parallel.
  *
  * VEP input files written to:
  *
  *  s3://dig-analysis-data/out/varianteffect/variants/<dataset>
  */
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
    masterInstanceType = Ec2.Strategy.memoryOptimized(mem = 128.gb),
    slaveInstanceType = Ec2.Strategy.memoryOptimized(mem = 64.gb),
    masterVolumeSizeInGB = 400,
    slaveVolumeSizeInGB = 400,
    instances = 8,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("list-variant-bootstrap.sh")))
  )

  /** Map inputs to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case datasetVariants() => Outputs.Named("variants")
    case ldServerVariants(_) => Outputs.Named("variants")
    case variantCounts(_, _, _) => Outputs.Named("variants")
  }

  /** All that matters is that there are new datasets. The input datasets are
    * actually ignored, and _everything_ is reprocessed. This is done because
    * there is only a single analysis node for all variants.
    */
  override def make(output: String): Job = {
    new Job(Job.PySpark(resourceUri("listVariants.py")))
  }
}
