package org.broadinstitute.dig.aggregator.methods.crediblesets

class C2CTStage(implicit context: Context) extends Stage {

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("download-cmdga.sh"))),
  )

  val credibleSets: Input.Source = Input.Source.Dataset("credible_sets/*/*/merged/")

  override val sources: Seq[Input.Source] = Seq(credibleSets)

  override val rules: PartialFunction[Input, Outputs] = {
    case credibleSets(phenotype, ancestry) => Outputs.Named(s"$phenotype/$ancestry")
  }

  override def make(output: String): Job = {
    val flags = output.split("/").toSeq match {
      case Seq(phenotype, ancestry) =>
        Seq(s"--phenotype=$phenotype", s"--ancestry=$ancestry")
    }

    new Job(Seq(Job.PySpark(resourceUri("C2CT.py"), flags:_*)))
  }
}
