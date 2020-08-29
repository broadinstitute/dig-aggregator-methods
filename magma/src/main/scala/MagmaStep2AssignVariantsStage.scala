package org.broadinstitute.dig.aggregator.methods.magma

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws.Ec2.Strategy
import org.broadinstitute.dig.aws.MemorySize
import org.broadinstitute.dig.aws.emr._

/**
  * Once all the distinct bi-allelic variants across all datasets have been
  * identified (VariantListProcessor) then they can be run through VEP in
  * parallel across multiple VMs.
  *
  * VEP TSV input files located at:
  *
  *  s3://dig-analysis-data/out/varianteffect/variants
  *
  * VEP output JSON written to:
  *
  *  s3://dig-analysis-data/out/varianteffect/effects
  */
class MagmaStep2AssignVariantsStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val variants: Input.Source = Input.Source.Success("out/magma/step1GatherVariants/")

  /** Additional resources that need uploaded to S3. */
  override def additionalResources: Seq[String] = Seq(
    "step2AssignVariantsToGenes.sh"
  )

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(variants)

  private lazy val step2Bootstrap = resourceUri("step2Bootstrap.sh")
  private lazy val installScript    = resourceUri("installVEP.sh")

  /** Definition of each VM "cluster" of 1 machine that will run magma.
    */
  override def cluster: ClusterDef = super.cluster.copy(
    masterInstanceType = Strategy.computeOptimized(vCPUs = 8),
    instances = 1,
    masterVolumeSizeInGB = 80,
    applications = Seq.empty,
    bootstrapScripts = Seq(new BootstrapScript(step2Bootstrap)),
  )

  /** Map inputs to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case variants() => Outputs.Named("Magma02")
  }

  /** The results are ignored, as all the variants are refreshed and everything
    * needs to be run through VEP again.
    */
  override def make(output: String): Job = {
    val runScript = resourceUri("step2AssignVariantsToGenes.sh")

    // get all the variant part files to process, use only the part filename
    // get all the variant part files to process, use only the part filename
    val objects = context.s3.ls(s"out/magma/step1GatherVariants/")
    val parts   = objects.map(_.key.split('/').last).filter(_.startsWith("part-"))

    // add a step for each part file
    new Job(parts.take(1).map(Job.Script(runScript, _)), isParallel = true)
  }

  /** Before the jobs actually run, perform this operation.
    */
  override def prepareJob(output: String): Unit = {
    context.s3.rm("out/magma/step2VariantToGene/")
  }

  /** On success, write the _SUCCESS file in the output directory.
    */
  override def success(output: String): Unit = {
    context.s3.touch("out/magma/step2VariantToGene/_SUCCESS")
    ()
  }
}
