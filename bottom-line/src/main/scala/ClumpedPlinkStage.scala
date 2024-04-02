package org.broadinstitute.dig.aggregator.methods.bottomline

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy

/** After meta-analysis, this stage finds the most significant variant
 * every 50 kb across the entire genome.
 */
class ClumpedPlinkStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val transEthnic: Input.Source = Input.Source.Success("out/metaanalysis/*/trans-ethnic/*/")
  val ancestrySpecific: Input.Source = Input.Source.Success("out/metaanalysis/*/ancestry-specific/*/*/")

  val paramTypes: Map[String, Seq[String]] = Map(
    "bottom-line" -> Seq("portal", "analysis"),
    "naive" -> Seq("analysis"),
    "min_p" -> Seq("analysis"),
    "largest" -> Seq("analysis")
  )

  /** The output of meta-analysis is the input for top associations. */
  override val sources: Seq[Input.Source] = Seq(transEthnic, ancestrySpecific)

  /** Process top associations for each phenotype. */
  override val rules: PartialFunction[Input, Outputs] = {
    case transEthnic(metaType, phenotype) =>
      Outputs.Named(paramTypes(metaType).map { paramType =>
        s"$metaType/$paramType/$phenotype"
      }: _*)
    case ancestrySpecific(metaType, phenotype, ancestry) =>
      Outputs.Named(paramTypes(metaType).map { paramType =>
        s"$metaType/$paramType/$phenotype/${ancestry.split("ancestry=").last}"
      }: _*)
  }

  /** Simple cluster with more memory. */
  override val cluster: ClusterDef = super.cluster.copy(
    masterInstanceType = Strategy.generalPurpose(mem = 64.gb),
    instances = 1,
    masterVolumeSizeInGB = 100,
    bootstrapSteps = Seq(Job.Script(resourceUri("install-plink.sh")))
  )

  /** Build the job. */
  override def make(output: String): Job = {
    // run clumping and then join with bottom line
    val flags = output.split("/").toSeq match {
      case Seq(metaType, paramType, phenotype) =>
        Seq(s"--phenotype=$phenotype", s"--ancestry=TE", s"--meta-type=$metaType", s"--param-type=$paramType")
      case Seq(metaType, paramType, phenotype, ancestry) =>
        Seq(s"--phenotype=$phenotype", s"--ancestry=$ancestry", s"--meta-type=$metaType", s"--param-type=$paramType")
    }

    val steps = Seq(
      Job.Script(resourceUri("runPlink.py"), flags:_*)
    )
    new Job(steps)
  }
}
