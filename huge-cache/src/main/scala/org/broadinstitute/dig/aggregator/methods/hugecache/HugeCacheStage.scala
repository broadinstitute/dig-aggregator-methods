package org.broadinstitute.dig.aggregator.methods.hugecache

import org.broadinstitute.dig.aggregator.core.{Context, Input, Outputs, Stage}
import org.broadinstitute.dig.aws.emr.{ClusterDef, Job}

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
class HugeCacheStage(implicit context: Context) extends Stage {

  val geneFile: String         = "genes/GRCh37/"
  val variantCqsDir: String    = "out/varianteffect/cqs/"
  val variantEffectDir: String = "out/varianteffect/effects/"
  val geneIdsMapDir: String    = "out/geneidmap/map/"
  val cacheDir                 = "out/huge/cache/"

  val genes: Input.Source          = Input.Source.Dataset(geneFile)
  val variantCqs: Input.Source     = Input.Source.Success(variantCqsDir)
  val variantEffects: Input.Source = Input.Source.Success(variantEffectDir)
  val geneIdsMap: Input.Source     = Input.Source.Success(geneIdsMapDir)

  /** Source inputs. */
  override val sources: Seq[Input.Source] = Seq(genes, variantCqs, variantEffects, geneIdsMap)

  /* Define settings for the cluster to run the job.
   */
  override val cluster: ClusterDef = {
    super.cluster.copy(instances = 3)
  }

  /** Map inputs to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case _ => Outputs.Named("HugeCache")
  }

  /** One job per phenotype (e.g. T2D)
    */
  override def make(output: String): Job = {
    val script = resourceUri("huge-cache.py")
    println(s"Making job with script $script, ignoring parameter $output.")
    val bucket = context.s3
    new Job(
      Job.PySpark(
        script,
        "--cqs",
        bucket.s3UriOf(variantCqsDir).toString,
        "--effects",
        bucket.s3UriOf(variantEffectDir).toString,
        "--gene-ids-map",
        bucket.s3UriOf(geneIdsMapDir).toString,
        "--cache-dir",
        bucket.s3UriOf(cacheDir).toString
      )
    )
  }
}
