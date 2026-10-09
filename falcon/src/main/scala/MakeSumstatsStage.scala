package org.broadinstitute.dig.aggregator.methods.falcon

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy

class MakeSumstatsStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val binBucket: S3.Bucket = new S3.Bucket("dig-analysis-data", None)
  val bottomLine: Input.Source = Input.Source.Success("out/metaanalysis/bottom-line/trans-ethnic/*/", s3BucketOverride=Some(binBucket))


  /** Source inputs. */
  override val sources: Seq[Input.Source] = Seq(bottomLine)

  /** Map inputs to their outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case bottomLine(phenotype) => Outputs.Named(phenotype)
  }

  /** Just need a single machine with no applications, but a good drive. */
  override def cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    applications = Seq.empty,
    masterVolumeSizeInGB = 100,
    masterInstanceType = Strategy.memoryOptimized(mem = 64.gb),
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("bootstrap_sumstats.sh"))),
    stepConcurrency = 8
  )

  override def make(output: String): Job = {
    new Job(Job.Script(resourceUri("makeSumstats.py"), s"--phenotype=$output"))
  }
}
