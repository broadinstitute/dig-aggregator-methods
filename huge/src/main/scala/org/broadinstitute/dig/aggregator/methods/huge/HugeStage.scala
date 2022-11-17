package org.broadinstitute.dig.aggregator.methods.huge

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
class HugeStage(implicit context: Context) extends Stage {

  val genomeBuild = "GRCh37"

  val genes: Input.Source            = Input.Source.Dataset(s"genes/$genomeBuild/")
  val geneAssociations: Input.Source = Input.Source.Dataset(s"gene_associations/52k_*/*/")
  val variants: Input.Source         = Input.Source.Success(s"out/metaanalysis/trans-ethnic/*/")

  /** Source inputs. */
  override val sources: Seq[Input.Source] = Seq(genes, geneAssociations, variants)

  val makeClusterSmall: Boolean = true

  /* Define settings for the cluster to run the job.
   */
  override val cluster: ClusterDef = {
    if (makeClusterSmall) {
      super.cluster.copy(instances = 1)
    } else {
      super.cluster.copy()
    }
  }

  /** Map inputs to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case genes()                        => Outputs.All
    case geneAssociations(_, phenotype) => Outputs.Named(phenotype)
    case variants(phenotype)            => Outputs.Named(phenotype)
  }

  /** One job per phenotype (e.g. T2D)
    */
  override def make(output: String): Job = {
    val script    = resourceUri("huge.py")
    val phenotype = output
    println(s"Making job with script $script for phenotype $phenotype.")
    new Job(Job.PySpark(script, s"--phenotype=$phenotype"))
  }
}
