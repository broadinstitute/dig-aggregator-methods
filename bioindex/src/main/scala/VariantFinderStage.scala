package org.broadinstitute.dig.aggregator.methods.bioindex

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

/** The final result of all aggregator methods is building the BioIndex. All
  * outputs are to the dig-bio-index bucket in S3.
  */
class VariantFinderStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val variants = Input.Source.Success("out/metaanalysis/trans-ethnic/")

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(variants)

  /** Rules for mapping input to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case variants() => Outputs.Named("bloom")
  }

  /** Use memory-optimized machine with sizeable disk space for shuffling. */
  override val cluster: ClusterDef = super.cluster.copy(
    masterInstanceType = Ec2.Strategy.memoryOptimized(mem = 64.gb),
    slaveInstanceType = Ec2.Strategy.memoryOptimized(mem = 64.gb),
    masterVolumeSizeInGB = 200,
    slaveVolumeSizeInGB = 200,
    instances = 6,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("cluster-bootstrap.sh")))
  )

  /** Output to Job steps. */
  override def make(output: String): Job = {
    val script = resourceUri("bloomFilter.py")
    val step   = Job.PySpark(script)

    new Job(Seq(step))
  }
}
