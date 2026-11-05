package org.broadinstitute.dig.aggregator.methods.falcon

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy

class PhewasStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    masterVolumeSizeInGB = 100,
    masterInstanceType = Strategy.memoryOptimized(mem = 64.gb),
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("bootstrap_phewas.sh"))),
    stepConcurrency = 8
  )

  val pigean: Input.Source = Input.Source.Raw("out/falcon/staging/eaggl/*/*/*/gene_clusters.out.gz")
  val gsCombined: Input.Source = Input.Source.Raw("out/falcon/staging/combined/*.tsv")

  override val sources: Seq[Input.Source] = Seq(pigean, gsCombined)

  var outputSet: Set[String] = Set[String]()

  override val rules: PartialFunction[Input, Outputs] = {
    case pigean(traitGroup, phenotype, model) =>
      outputSet += s"$traitGroup/$phenotype/$model"
      Outputs.Null
    case gsCombined(_) => Outputs.Named("phewas")
  }

  def toFlags(output: String): Seq[String] = output.split("/").toSeq match {
    case Seq(traitGroup, phenotype, model) =>
      Seq(s"--trait-group=$traitGroup", s"--phenotype=$phenotype", s"--model=$model")
  }

  def filterByTrait(traitGroup: String): String => Boolean = {
    input: String => input.split("/").toSeq match {
      case Seq(inputTraitGroup, _, _) if inputTraitGroup == traitGroup => true
      case _ => false
    }
  }

  val phenotypes: Set[String] = Set()

  def filterByPhenotype(): String => Boolean = {
    input: String => input.split("/").toSeq match {
      case Seq(_, phenotype, _) if phenotypes.contains(phenotype) => true
      case _ => false
    }
  }

  override def make(output: String): Job = {
    val steps: Seq[Job.Script] = outputSet.map { output =>
      Job.Script(resourceUri("runPhewas.py"), toFlags(output):_*)
    }.toSeq
    new Job(steps, parallelSteps = true)
  }
}
