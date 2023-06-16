package org.broadinstitute.dig.aggregator.methods.ldsc

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy
import org.broadinstitute.dig.aws.MemorySize

class TranslatePartitionedHeritabilityStage(implicit context: Context) extends Stage {

  val partitioned_heritability: Input.Source = Input.Source.Success("out/ldsc/staging/partitioned_heritability/")

  /** Source inputs. */
  override val sources: Seq[Input.Source] = Seq(partitioned_heritability)

  /** Map inputs to their outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case partitioned_heritability() => Outputs.Named("translate")
  }

  /** Just need a single machine with no applications, but a good drive. */
  override def cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    applications = Seq.empty,
    bootstrapScripts = Seq(
      new BootstrapScript(resourceUri("install-translate-ph.sh"))
    ),
    releaseLabel = ReleaseLabel("emr-6.7.0")
  )

  override def make(output: String): Job = {
    new Job(Job.Script(resourceUri("translatePartitionedHeritability.py")))
  }

  /** Before the jobs actually run, perform this operation.
   */
  override def prepareJob(output: String): Unit = {
    context.s3.rm(s"out/ldsc/partitioned_heritability/")
  }

  /** On success, write the _SUCCESS file in the output directory.
   */
  override def success(output: String): Unit = {
    context.s3.touch(s"out/ldsc/partitioned_heritability/_SUCCESS")
    ()
  }
}

