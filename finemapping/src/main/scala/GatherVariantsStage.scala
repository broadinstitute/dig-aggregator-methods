package org.broadinstitute.dig.aggregator.methods.finemapping

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy

/** Join ancestry specific phenotyp variant data and join with frequency data
  * Output into csv for cojo analysis
  */
class GatherVariantsStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val bottomLine: Input.Source = Input.Source.Success("out/metaanalysis/trans-ethnic/*/")
  val snps: Input.Source       = Input.Source.Success("out/finemapping/variant-frequencies/")

  /** Ouputs to watch. */
  override val sources: Seq[Input.Source] = Seq(bottomLine)

  /** Process top associations for each phenotype. */
  override val rules: PartialFunction[Input, Outputs] = {
    case bottomLine(phenotype) => Outputs.Named(phenotype)
  }

  /** Simple cluster with more memory. */
  override val cluster: ClusterDef = super.cluster.copy(
    masterInstanceType = Strategy.generalPurpose(mem = 64.gb),
    slaveInstanceType = Strategy.generalPurpose(mem = 32.gb),
    instances = 4
  )

  /** Build the job. */
  override def make(output: String): Job = {
    val script    = resourceUri("gatherVariantsByPhenotype.py")
    val phenotype = output

    new Job(Job.PySpark(script, phenotype))
  }
}