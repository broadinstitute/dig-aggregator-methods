package org.broadinstitute.dig.aggregator.methods.bioindex

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

/** The final result of all aggregator methods is building the BioIndex. All
  * outputs are to the dig-bio-index bucket in S3.
  */
class TopAssociationsStage(implicit context: Context) extends Stage {
  val clumped: Input.Source = Input.Source.Success("out/metaanalysis/clumped/")
  val ancestryClumped: Input.Source = Input.Source.Success("out/metaanalysis/ancestry-clumped/*/*/")
  val datasetClumped: Input.Source = Input.Source.Success("out/metaanalysis/dataset-clumped/*/*/")

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(clumped, ancestryClumped, datasetClumped)

  /** Rules for mapping input to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case clumped() => Outputs.Named("clumped")
    case ancestryClumped(_, ancestry) => Outputs.Named(ancestry.split("=").last)
    case datasetClumped(_, _) => Outputs.Named("clumped")
  }

  /** Output to Job steps. */
  override def make(output: String): Job = {
    output match {
      case "clumped" => new Job(Job.PySpark(resourceUri("topAssociations.py"), s"--ancestry=Mixed"))
      case ancestry => new Job(Job.PySpark(resourceUri("topAssociations.py"), s"--ancestry=$ancestry"))
    }

  }
}
