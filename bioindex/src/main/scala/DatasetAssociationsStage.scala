package org.broadinstitute.dig.aggregator.methods.bioindex

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

/** The final result of all aggregator methods is building the BioIndex. All
  * outputs are to the dig-bio-index bucket in S3.
  */
class DatasetAssociationsStage(implicit context: Context) extends Stage {
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
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("cluster-bootstrap-6.7.0.sh"))),
    releaseLabel = ReleaseLabel("emr-6.7.0") // Need emr 6.1+ to write json files properly
  )

  /** Output to Job steps. */
  override def make(output: String): Job = {
    val associations = resourceUri("datasetAssociations.py")
    val plot         = resourceUri("plotAssociations.py")

    // get best associations and build plots
    val steps = Seq(
      Job.PySpark(associations, output),
      Job.Script(plot, s"--dataset=$output")
    )

    new Job(steps)
  }
}
