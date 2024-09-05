package org.broadinstitute.dig.aggregator.methods.ldsc

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy
import org.broadinstitute.dig.aws.MemorySize

class TranslateGeneticCorrelationStage(implicit context: Context) extends Stage {

  val correlations: Input.Source = Input.Source.Success("out/ldsc/staging/genetic_correlation/*/*/")

  /** Source inputs. */
  override val sources: Seq[Input.Source] = Seq(correlations)

  /** Map inputs to their outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case correlations(_, _) => Outputs.Named("translate")
  }

  /** Just need a single machine with no applications, but a good drive. */
  override def cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    applications = Seq.empty,
    bootstrapScripts = Seq(
      new BootstrapScript(resourceUri("install-translate.sh")),
      new BootstrapScript(
        resourceUri("downloadGCFiles.py"), s"--input-path=s3://${context.s3.path}"
      )
    )
  )

  override def make(output: String): Job = {
    new Job(Job.Script(resourceUri("translateGeneticCorrelation.py")))
  }

  /** Before the jobs actually run, perform this operation.
   */
  override def prepareJob(output: String): Unit = {
    context.s3.rm(s"out/ldsc/genetic_correlation/")
  }

  /** On success, write the _SUCCESS file in the output directory.
   */
  override def success(output: String): Unit = {
    context.s3.touch(s"out/ldsc/genetic_correlation/_SUCCESS")
    ()
  }
}
