package org.broadinstitute.dig.aggregator.methods.bioindex

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

/** The final result of all aggregator methods is building the BioIndex. All
  * outputs are to the dig-bio-index bucket in S3.
  */
class ClumpedVariantsStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val clumped: Input.Source = Input.Source.Success("out/metaanalysis/bottom-line/clumped/*/")
  val ancestryClumped: Input.Source = Input.Source.Success("out/metaanalysis/bottom-line/ancestry-clumped/*/*/")

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(clumped, ancestryClumped)

  /** Rules for mapping input to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case clumped(_) => Outputs.Named("clumps")
    case ancestryClumped(_, ancestry) => Outputs.Named(ancestry.split("=").last)
  }

  /** Use memory-optimized machine with sizeable disk space for shuffling. */
  override val cluster: ClusterDef = super.cluster.copy(
    masterInstanceType = Ec2.Strategy.generalPurpose(mem = 64.gb),
    slaveInstanceType = Ec2.Strategy.generalPurpose(mem = 64.gb),
    masterVolumeSizeInGB = 100,
    slaveVolumeSizeInGB = 100,
    instances = 5,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("cluster-bootstrap.sh")))
  )

  /** Output to Job steps. */
  override def make(output: String): Job = {
    val flag = output match {
      case "clumps" => "--ancestry=Mixed"
      case ancestry => s"--ancestry=$ancestry"
    }
    val steps = Seq(
      Job.PySpark(resourceUri("clumpedVariants.py"), flag),
      Job.PySpark(resourceUri("clumpedAssociationsMatrix.py"), flag)
    )

    new Job(steps)
  }
}
