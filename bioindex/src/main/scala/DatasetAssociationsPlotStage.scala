package org.broadinstitute.dig.aggregator.methods.bioindex

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

/** The final result of all aggregator methods is building the BioIndex. All
 * outputs are to the dig-bio-index bucket in S3.
 */
class DatasetAssociationsPlotStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val variants = Input.Source.Success("variants/*/*/*/")

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(variants)

  /** Rules for mapping input to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case variants(tech, dataset, phenotype) => Outputs.Named(s"$tech/$dataset/$phenotype")
  }

  /** Use memory-optimized machine with sizeable disk space for shuffling. */
  override val cluster: ClusterDef = super.cluster.copy(
    masterInstanceType = Ec2.Strategy.memoryOptimized(),
    masterVolumeSizeInGB = 200,
    instances = 1,
    applications = Seq.empty,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("cluster-bootstrap.sh")))
  )

  /** Output to Job steps. */
  override def make(output: String): Job = {
    val steps = Seq(Job.Script(resourceUri("plotAssociations.py"), s"--dataset=$output"))
    new Job(steps)
  }
}
