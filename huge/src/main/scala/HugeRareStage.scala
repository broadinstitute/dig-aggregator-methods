package org.broadinstitute.dig.aggregator.methods.huge

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

class HugeRareStage(implicit context: Context) extends Stage {

  val geneAssociations: Input.Source = Input.Source.Success("gene_associations/combined/*/")

  override val sources: Seq[Input.Source] = Seq(geneAssociations)

  override val cluster: ClusterDef = {
    super.cluster.copy(
      instances = 1,
      bootstrapScripts = Seq(new BootstrapScript(resourceUri("huge-rare-bootstrap.sh")))
    )
  }

  override val rules: PartialFunction[Input, Outputs] = {
    case geneAssociations(phenotype) => Outputs.Named(phenotype)
  }

  override def make(output: String): Job = {
    new Job(Job.PySpark(resourceUri("huge-rare.py"), s"--phenotype=$output")
    )
  }
}
