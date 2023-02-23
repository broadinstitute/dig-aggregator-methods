package org.broadinstitute.dig.aggregator.methods.geneidmap

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
class GeneIdMapStage(implicit context: Context) extends Stage {

  val genesDir: String          = "genes/GRCh37/"
  val variantEffectsDir: String = "out/varianteffect/effects/"
  val genesOutDir               = "out/geneidmap/genes/"
  val geneIdsMapDir             = "out/geneidmap/map/"

  val genes: Input.Source          = Input.Source.Dataset(genesDir)
  val variantEffects: Input.Source = Input.Source.Success(variantEffectsDir)

  /** Source inputs. */
  override val sources: Seq[Input.Source] = Seq(genes, variantEffects)

  /* Define settings for the cluster to run the job.
   */
  override val cluster: ClusterDef = {
    super.cluster.copy(
      instances = 3,
      bootstrapScripts = Seq(new BootstrapScript(resourceUri("gene-id-map-bootstrap.sh"))),
      releaseLabel = ReleaseLabel("emr-6.7.0")
    )
  }

  /** Map inputs to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case _ => Outputs.Named("GeneIdMap")
  }

  /** One job per phenotype (e.g. T2D)
    */
  override def make(output: String): Job = {
    val script = resourceUri("gene-id-map.py")
    println(s"Making job with script $script, ignoring parameter $output.")
    val bucket = context.s3
    new Job(
      Job.PySpark(
        script,
        "--genes-dir",
        bucket.s3UriOf(genesDir).toString,
        "--variant-effects-dir",
        bucket.s3UriOf(variantEffectsDir).toString,
        "--genes-out-dir",
        bucket.s3UriOf(genesOutDir).toString,
        "--map-dir",
        bucket.s3UriOf(geneIdsMapDir).toString
      )
    )
  }
}
