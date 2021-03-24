package org.broadinstitute.dig.aggregator.methods.ldsc

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy
import org.broadinstitute.dig.aws.MemorySize

class MungeStatsStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val bottomLine: Input.Source = Input.Source.Success("out/metaanalysis/trans-ethnic/*/")

  /** Source inputs. */
  override val sources: Seq[Input.Source] = Seq(bottomLine)

  /** Map inputs to their outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case bottomLine(phenotype) => Outputs.Named(phenotype)
  }

  /** Just need a single machine with no applications, but a good drive. */
  override def cluster: ClusterDef = super.cluster.copy(
    masterInstanceType = Strategy.memoryOptimized(mem = 64.gb),
    instances = 1,
    masterVolumeSizeInGB = 200,
    applications = Seq.empty,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("install-ldsc.sh"))),
    stepConcurrency = 3
  )

  /** For each phenotype, load the METAL output and pass it to LDSC's
    * munge stats python script. Upload the results to S3.
    */
  override def make(output: String): Job = {
    new Job(Job.Script(resourceUri("mungeStats.sh"), output))
  }

  /** Before the jobs actually run, perform this operation.
    */
  override def prepareJob(output: String): Unit = {
    context.s3.rm(s"out/ldsc/sumstats/$output/")
  }

  /** On success, write the _SUCCESS file in the output directory.
    */
  override def success(output: String): Unit = {
    context.s3.touch(s"out/ldsc/sumstats/$output/_SUCCESS")
    ()
  }
}
