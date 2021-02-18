package org.broadinstitute.dig.aggregator.methods.bottomline

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy

/** After meta-analysis, this stage finds the most significant variant
  * every 50 kb across the entire genome.
  */
class TopAssociationsStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val bottomLine: Input.Source = Input.Source.Success("out/metaanalysis/trans-ethnic/*/")
  val snps: Input.Source       = Input.Source.Success("out/varianteffect/snp/")

  /** The output of meta-analysis is the input for top associations. */
  override val sources: Seq[Input.Source] = Seq(bottomLine, snps)

  /** Process top associations for each phenotype. */
  override val rules: PartialFunction[Input, Outputs] = {
    case bottomLine(phenotype) => Outputs.Named(phenotype)
    case snps()                => Outputs.All
  }

  /** Simple cluster with more memory. */
  override val cluster: ClusterDef = super.cluster.copy(
    masterInstanceType = Strategy.generalPurpose(mem = 64.gb),
    instances = 1,
    masterVolumeSizeInGB = 400,
    bootstrapSteps = Seq(Job.Script(resourceUri("install-plink.sh")))
  )

  /** Build the job. */
  override def make(output: String): Job = {
    val phenotype = output

    // run clumping and then join with bottom line
    val steps = Seq(
      Job.Script(resourceUri("runPlink.py"), phenotype),
      Job.PySpark(resourceUri("topAssociations.py"), phenotype)
    )

    new Job(steps)
  }

  /** Nuke the staging directories before the job runs.
    */
  override def prepareJob(output: String): Unit = {
    context.s3.rm(s"out/metaanalysis/staging/clumped/$output/")
    context.s3.rm(s"out/metaanalysis/staging/plink/$output/")
  }
}
