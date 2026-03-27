package org.broadinstitute.dig.aggregator.methods.pigean

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

class GeneSetOverlapStage(implicit context: Context) extends Stage {

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("overlap-bootstrap.sh")))
  )

  val geneList: Input.Source = Input.Source.Raw("out/pigean/gene_lists/*/*")

  override val sources: Seq[Input.Source] = Seq(geneList)

  override val rules: PartialFunction[Input, Outputs] = {
    case geneList(listGroup, _) => Outputs.Named(listGroup)
  }

  override def make(output: String): Job = {
    new Job(Job.Script(resourceUri("geneSetOverlap.py"), s"--list-group=$output"))
  }
}

