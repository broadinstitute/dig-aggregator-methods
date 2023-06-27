package org.broadinstitute.dig.aggregator.methods.geneassociations

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

class CombineAssociationsStage(implicit context: Context) extends Stage {

  override val cluster: ClusterDef = super.cluster.copy(
    applications = Seq.empty,
    instances = 1,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("intake_bootstrap.sh"))),
    releaseLabel = ReleaseLabel("emr-6.7.0"),
    stepConcurrency = 10
  )

  val t2d_52k = Input.Source.Dataset("gene_associations/52k_T2D/*/")
  val qt_52k  = Input.Source.Dataset("gene_associations/52k_QT/*/")
  val traits_600 = Input.Source.Success("gene_associations/600k_combined/*/")
  val genebass = Input.Source.Dataset("gene_associations/genebass/*/")

  override val sources: Seq[Input.Source] = Seq(t2d_52k, qt_52k, traits_600, genebass)

  override val rules: PartialFunction[Input, Outputs] = {
    case t2d_52k(phenotype) => Outputs.Named(phenotype)
    case qt_52k(phenotype) => Outputs.Named(phenotype)
    case traits_600(phenotype) => Outputs.Named(phenotype)
    case genebass(phenotype) => Outputs.Named(phenotype)
  }

  override def make(output: String): Job = {
    new Job(Job.Script(resourceUri("combineAssociations.py"), s"--phenotype=$output"))
  }

  override def prepareJob(output: String): Unit = {
    context.s3.rm(s"gene_associations/combined/$output/")
  }
}
