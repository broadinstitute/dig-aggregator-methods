package org.broadinstitute.dig.aggregator.methods.bottomline

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy

class HierarchicalMetalStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val gc: Input.Source = Input.Source.Success("out/ldsc/staging/dataset_genetic_correlation/*/*/")

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    applications = Seq.empty,
    masterVolumeSizeInGB = 100,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("bootstrap-hierarchical.sh")))
  )

  override val sources: Seq[Input.Source] = Seq(gc)


  override val rules: PartialFunction[Input, Outputs] = {
    case gc(phenotype, ancestry) => Outputs.Named(s"$phenotype/$ancestry")
  }

  override def make(output: String): Job = {
    val flags = output.split("/").toSeq match {
      case Seq(phenotype, ancestry) =>
        Seq(s"--phenotype=$phenotype", s"--ancestry=$ancestry")
    }

    new Job(Job.Script(resourceUri("runHierarchicalMetal.py"), flags:_*))
  }
}
