package org.broadinstitute.dig.aggregator.methods.nearestgene

import org.broadinstitute.dig.aggregator.core.{Context, Input, Outputs, Stage}
import org.broadinstitute.dig.aws.emr.{BootstrapScript, ClusterDef, Job, ReleaseLabel}

/** This is a stage in your method.
  *
  * Stages take one or more inputs and generate one or more outputs. Each
  * stage consists of a...
  *
  *   - list of input sources;
  *   - rules mapping inputs to outputs;
  *   - make function that returns a job used to produce a given output
  *
  * Optionally, a stage can also override...
  *
  *   - its name, which defaults to its class name
  *   - the cluster definition used to provision EC2 instances
  */
class NearestGeneStage(implicit context: Context) extends Stage {

  val genesDir: String    = "out/geneidmap/genes/"
  val variantsDir: String = "out/varianteffect/cqs/"
  val outDir              = "out/nearestgenes/"

  val genes: Input.Source    = Input.Source.Dataset(genesDir)
  val variants: Input.Source = Input.Source.Dataset(variantsDir)

  /** Source inputs. */
  override val sources: Seq[Input.Source] = Seq(genes, variants)

  /* Define settings for the cluster to run the job.
   */
  override val cluster: ClusterDef = {
    super.cluster.copy(
      instances = 3,
      releaseLabel = ReleaseLabel("emr-6.7.0")
    )
  }

  /** Map inputs to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case _ => Outputs.Named("NearestGene")
  }

  /** One job per phenotype (e.g. T2D)
    */
  override def make(output: String): Job = {
    val script = resourceUri("nearest-gene.py")
    println(s"Making job with script $script, ignoring parameter $output.")
    val bucket = context.s3
    new Job(
      Job.PySpark(
        script,
        "--genes-dir",
        bucket.s3UriOf(genesDir).toString,
        "--variants-dir",
        bucket.s3UriOf(variantsDir).toString,
        "--out-dir",
        bucket.s3UriOf(outDir).toString
      )
    )
  }
}
