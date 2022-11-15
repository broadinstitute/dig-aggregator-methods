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

  val genes: Input.Source = Input.Source.Success(s"genes/$genomeBuild/part-*")
  val geneAssociations: Input.Source = Input.Source.Success(s"out/magma/gene-associations/*/ancestry=Mixed/part-*.json")
  val variants: Input.Source = Input.Source.Success(s"/out/metaanalysis/trans-ethnic/*/part-*")

  /** Source inputs. */
  override val sources: Seq[Input.Source] = Seq(genes, geneAssociations, variants)

  /* Define settings for the cluster to run the job.
   */
  override val cluster: ClusterDef = super.cluster.copy()

  /** Map inputs to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case genes(_) => Outputs.All
    case geneAssociations(phenotype, _) => Outputs.Named(phenotype)
    case variants(phenotype, _) => Outputs.Named(phenotype)
  }

  /** All that matters is that there are new datasets. The input datasets are
    * actually ignored, and _everything_ is reprocessed. This is done because
    * there is only a single analysis node for all variants.
    */
  override def make(output: String): Job = {
    new Job(Job.PySpark(resourceUri("huge.py")))
  }
}
