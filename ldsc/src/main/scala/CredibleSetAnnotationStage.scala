package org.broadinstitute.dig.aggregator.methods.ldsc

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws.emr._

class CredibleSetAnnotationStage(implicit context: Context) extends Stage {

  val regions: Input.Source = Input.Source.Success("out/ldsc/regions/merged/annotation-tissue-biosample/*/")
  val credibleSets: Input.Source = Input.Source.Dataset("credible_sets/*/*/")

  /** Source inputs. */
  override val sources: Seq[Input.Source] = Seq(regions, credibleSets)

  /** Just need a single machine with no applications, but a good drive. */
  override def cluster: ClusterDef = super.cluster.copy(
    releaseLabel = ReleaseLabel("emr-6.7.0")
  )


  override val rules: PartialFunction[Input, Outputs] = {
    case credibleSets(dataset, phenotype) => Outputs.Named(CredibleSetOutput(dataset, phenotype).toOutput)
    case regions(_) => Outputs.All
  }

  override def make(output: String): Job = {
    val credibleSetOutput = CredibleSetOutput.fromOutput(output)
    new Job(
      Job.PySpark(
        resourceUri("credibleSetAnnotation.py"),
        s"--credible-set-path=${credibleSetOutput.dataset}/${credibleSetOutput.phenotype}"
      )
    )
  }
}

case class CredibleSetOutput(
  dataset: String,
  phenotype: String
) {
  def toOutput: String = s"credible_set/$dataset/$phenotype"
}

object CredibleSetOutput {
  def fromOutput(output: String): CredibleSetOutput = output.split("/").toSeq match {
    case Seq("credible_set", dataset, phenotype) => CredibleSetOutput(dataset, phenotype)
    case _ => throw new Exception("Invalid String")
  }
}
