package org.broadinstitute.dig.aggregator.methods.pigean

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy

class PhewasPigeanStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    masterVolumeSizeInGB = 100,
    masterInstanceType = Strategy.generalPurpose(mem = 64.gb),
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("phewas-bootstrap.sh"))),
    stepConcurrency = 8
  )

  val pigean: Input.Source = Input.Source.Success("out/pigean/staging/factor/*/*/*/")
  val gsCombined: Input.Source = Input.Source.Raw("out/pigean/staging/combined_gs/*.tsv")

  override val sources: Seq[Input.Source] = Seq(pigean, gsCombined)

  var outputSet: Set[String] = Set[String]()

  override val rules: PartialFunction[Input, Outputs] = {
    case pigean(traitGroup, phenotype, geneSetSize) =>
      outputSet += s"$traitGroup/$phenotype/$geneSetSize"
      Outputs.Null
    case gsCombined(_) => Outputs.Named("phewas")
  }

  def toFlags(output: String): Seq[String] = output.split("/").toSeq match {
    case Seq(traitGroup, phenotype, geneSetSize) =>
      Seq(s"--trait-group=$traitGroup", s"--phenotype=$phenotype", s"--gene-set-size=$geneSetSize")
  }

  def filterByTrait(traitGroup: String): String => Boolean = {
    input: String => input.split("/").toSeq match {
      case Seq(inputTraitGroup, _, _, _) if inputTraitGroup == traitGroup => true
      case _ => false
    }
  }

  val phenotypes: Set[String] = Set()

  def filterByPhenotype(): String => Boolean = {
    input: String => input.split("/").toSeq match {
      case Seq(_, phenotype, _, _) if phenotypes.contains(phenotype) => true
      case _ => false
    }
  }

  override def make(output: String): Job = {
    val steps: Seq[Job.Script] = outputSet.map { output =>
      Job.Script(resourceUri("phewasPigean.py"), toFlags(output):_*)
    }.toSeq
    new Job(steps, parallelSteps = true)
  }
}
