package org.broadinstitute.dig.aggregator.methods.bottomline

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy

/** After meta-analysis, this stage finds the most significant variant
  * every 50 kb across the entire genome.
  */
class ClumpedAssociationsStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val portalPlink: Input.Source = Input.Source.Raw("out/metaanalysis/*/staging/clumped/portal/*/*/variants.json")

  /** The output of meta-analysis is the input for top associations. */
  override val sources: Seq[Input.Source] = Seq(portalPlink)

  /** Process top associations for each phenotype. */
  override val rules: PartialFunction[Input, Outputs] = {
    case portalPlink(metaType, phenotype, ancestry) =>
      Outputs.Named(s"$metaType/$phenotype/${ancestry.split("ancestry=").last}")
  }

  /** Simple cluster with more memory. */
  override val cluster: ClusterDef = super.cluster.copy(
    masterInstanceType = Strategy.generalPurpose(mem = 64.gb),
    instances = 1
  )

  /** Build the job. */
  override def make(output: String): Job = {
    // run clumping and then join with bottom line
    val flags = output.split("/").toSeq match {
      case Seq(metaType, phenotype, ancestry) =>
        Seq(s"--phenotype=$phenotype", s"--ancestry=$ancestry", s"--meta-type=$metaType", s"--param-type=portal")
    }

    val steps = Seq(
      Job.PySpark(resourceUri("clumpedAssociations.py"), flags:_*)
    )
    new Job(steps)
  }
}
