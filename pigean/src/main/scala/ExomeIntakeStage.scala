package org.broadinstitute.dig.aggregator.methods.pigean

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

class ExomeIntakeStage(implicit context: Context) extends Stage {

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1
  )

  val geneAssociations: Input.Source = Input.Source.Success("gene_associations/combined/*/")

  override val sources: Seq[Input.Source] = Seq(geneAssociations)

  override val rules: PartialFunction[Input, Outputs] = {
    case geneAssociations(phenotype) => Outputs.Named(phenotype)
  }

  override def make(output: String): Job = {
    new Job(Job.Script(resourceUri("exomeIntake.py"), s"--phenotype=$output"))
  }
}
