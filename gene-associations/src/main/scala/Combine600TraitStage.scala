package org.broadinstitute.dig.aggregator.methods.geneassociations

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

class Combine600TraitStage(implicit context: Context) extends Stage {

  override val cluster: ClusterDef = super.cluster.copy(
    applications = Seq.empty,
    instances = 1
  )

  val traits_600 = Input.Source.Dataset("gene_associations/600k_600traits/*/")

  override val sources: Seq[Input.Source] = Seq(traits_600)

  override val rules: PartialFunction[Input, Outputs] = {
    case traits_600(phenotype) => Outputs.Named("combine")
  }

  override def make(output: String): Job = {
    new Job(Job.Script(resourceUri("combine600Trait.py")))
  }

  override def prepareJob(output: String): Unit = {
    context.s3.rm(s"gene_associations/600k_combined/")
  }
}
