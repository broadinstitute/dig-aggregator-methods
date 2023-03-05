package org.broadinstitute.dig.aggregator.methods.ldsc

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy
import org.broadinstitute.dig.aws.MemorySize

class GeneticCorrelationStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val sumstats: Input.Source = Input.Source.Success("out/ldsc/sumstats/*/*/")

  /** Source inputs. */
  override val sources: Seq[Input.Source] = Seq(sumstats)

  /** Map inputs to their outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case sumstats(_, ancestry) => Outputs.Named(ancestry.split('=').last)
  }

  /** Just need a single machine with no applications, but a good drive. */
  override def cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    applications = Seq.empty,
    masterVolumeSizeInGB = 100,
    bootstrapScripts = Seq(
      new BootstrapScript(resourceUri("install-gc.sh")),
      new BootstrapScript(resourceUri("downloadSumstatsFiles.py"))
    ),
    stepConcurrency = 15,
    masterInstanceType = Strategy.generalPurpose(mem = 64.gb)
  )

  override def make(output: String): Job = {
    // Extract phenotype for only sumstats files for desired ancestry
    val pattern = s".*/([^/]+)/ancestry=$output/.*\\.sumstats\\.gz".r
    val jobs = context.s3.ls(s"out/ldsc/sumstats/").flatMap { keyObject =>
      keyObject.key match {
        case pattern(phenotype) =>
          Some(Job.Script(resourceUri("runGeneticCorrelation.py"), s"--phenotype=$phenotype", s"--ancestry=$output"))
        case _ => None
      }
    }

    new Job(jobs, parallelSteps = true)
  }

  /** Before the jobs actually run, perform this operation.
   */
  override def prepareJob(output: String): Unit = {
    context.s3.rm(s"out/ldsc/staging/genetic_correlation/ancestry=$output/")
  }

  /** On success, write the _SUCCESS file in the output directory.
   */
  override def success(output: String): Unit = {
    context.s3.touch(s"out/ldsc/staging/genetic_correlation/ancestry=$output/_SUCCESS")
    ()
  }
}
