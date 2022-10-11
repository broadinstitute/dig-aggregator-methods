package org.broadinstitute.dig.aggregator.methods.bioindex

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

class GeneToTranscriptStage(implicit context: Context) extends Stage {
  val geneToTranscript = Input.Source.Raw("raw/gencode.v36lift37.annotation.gtf.gz")

  override val cluster: ClusterDef = super.cluster.copy(
    applications = Seq.empty,
    instances = 1
  )

  override val sources: Seq[Input.Source] = Seq(geneToTranscript)

  override val rules: PartialFunction[Input, Outputs] = {
    case geneToTranscript() => Outputs.Named("geneToTranscript")
  }

  override def make(output: String): Job = {
    new Job(Job.Script(resourceUri("geneToTranscript.py")))
  }
}
