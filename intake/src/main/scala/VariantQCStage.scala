package org.broadinstitute.dig.aggregator.methods.intake

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

/** Variant QC Stage
  *
  * This consumes the output from loamstream and will run a series of QC methods on the data to output a final
  * "clean" dataset (along with datasets representing "bad" lines from each dataset).
  */
class VariantQCStage(implicit context: Context) extends Stage {

  /** No extra bootstrap for now
    * Default parameters:
    *   - Master node: m5.2xlarge; 8 vCore, 32 GiB memory, EBS only storage;  EBS Storage:32 GiB
    *   - Core nodes (instances - 1): m5.2xlarge; 8 vCore, 32 GiB memory, EBS only storage; EBS Storage:32 GiB
    */
  override val cluster: ClusterDef = super.cluster.copy(
    instances = 2
  )

  /** Input sources need to be declared so they can be used in rules.
    *
    * apply to all method/dataset/phenotype groups individually
    */
  val variants: Input.Source = Input.Source.Dataset("variants_processed/*/*/*/")

  override val sources: Seq[Input.Source] = Seq(variants)

  /** We can QC each dataset individually, and will save to a QC folder for each as well
    */
  override val rules: PartialFunction[Input, Outputs] = {
    case variants(method, dataset, phenotype) => Outputs.Named(s"${method}/${dataset}/${phenotype}")
  }

  /** Each method/dataset/variant will be processed individually and in parallel
    */
  override def make(output: String): Job = {

    val variantQCJob = resourceUri("variantQC.py")

    val methodDatasetPhenotype = output

    // list of qc steps to run (currently we are only running QC on variants)
    val steps = Seq(
      Job.PySpark(variantQCJob, methodDatasetPhenotype)
    )

    new Job(steps)
  }
}
