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

  val transEthnic: Input.Source = Input.Source.Raw("out/metaanalysis/*/staging/clumped/*/variants.json")
  val ancestrySpecific: Input.Source = Input.Source.Raw("out/metaanalysis/*/staging/ancestry-clumped/*/*/variants.json")

  val paramTypes: Map[String, Seq[String]] = Map(
    "bottom-line" -> Seq("portal")
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
    instances = 1
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
      Job.PySpark(resourceUri("clumpedAssociations.py"), flags:_*)
    )
    new Job(steps)
  }
}
