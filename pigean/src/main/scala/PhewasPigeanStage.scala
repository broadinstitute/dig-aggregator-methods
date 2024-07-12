package org.broadinstitute.dig.aggregator.methods.pigean

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

class PhewasPigeanStage(implicit context: Context) extends Stage {

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("phewas-bootstrap.sh"))),
    stepConcurrency = 8
  )

  val pigean: Input.Source = Input.Source.Success("out/pigean/staging/factor/*/*/*/")
  val gsCombined: Input.Source = Input.Source.Raw("out/pigean/staging/combined_gs/*.tsv")

  override val sources: Seq[Input.Source] = Seq(pigean, gsCombined)

  var outputSet: Set[String] = Set[String]()

  override val rules: PartialFunction[Input, Outputs] = {
    case pigean(phenotype, sigmaPower, geneSetSize) =>
      outputSet += s"$phenotype/${sigmaPower.split("sigma=").last}/${geneSetSize.split("size=").last}"
      Outputs.Null
    case gsCombined(_) => Outputs.Named("phewas")
  }

  def toFlags(output: String): Seq[String] = output.split("/").toSeq match {
    case Seq(phenotype, sigmaPower, geneSetSize) =>
      Seq(s"--phenotype=$phenotype", s"--sigma=$sigmaPower", s"--gene-set-size=$geneSetSize")
  }

  override def make(output: String): Job = {
    val steps: Seq[Job.Script] = outputSet.map { output =>
      Job.Script(resourceUri("phewasPigean.py"), toFlags(output):_*)
    }.toSeq
    new Job(steps, parallelSteps = true)
  }
}
