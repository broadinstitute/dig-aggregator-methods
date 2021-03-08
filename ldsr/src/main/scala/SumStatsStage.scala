package org.broadinstitute.dig.aggregator.methods.ldsr

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws.emr._

class SumStatsStage(implicit context: Context) extends Stage {
  val bottomLine: Input.Source = Input.Source.Success("out/metaanalysis/trans-ethnic/*/")

  /** Source inputs. */
  override val sources: Seq[Input.Source] = Seq(bottomLine)

  /** Map inputs to their outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case bottomLine(phenotype) => Outputs.Named(phenotype)
  }

  /** Just need a single machine with no applications, but a good drive. */
  override def cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    masterVolumeSizeInGB = 200,
    applications = Seq.empty,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("install-ldsc.sh"))),
    stepConcurrency = 5
  )

  /** Take any new datasets and convert them from JSON-list to BED file
    * format with all the appropriate headers and fields. All the datasets
    * are processed together by the Spark job, so what's in the results
    * input doesn't matter.
    */
  override def make(output: String): Job = {
    new Job(Job.Script(resourceUri("buildSumStats.sh"), output))
  }
}
