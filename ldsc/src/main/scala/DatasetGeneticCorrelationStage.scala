package org.broadinstitute.dig.aggregator.methods.ldsc

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy
import org.broadinstitute.dig.aws.MemorySize

class DatasetGeneticCorrelationStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val sumstats: Input.Source = Input.Source.Raw("out/ldsc/dataset_sumstats/*/*/*/ldsc.sumstats.gz")

  /** Source inputs. */
  override val sources: Seq[Input.Source] = Seq(sumstats)

  override val rules: PartialFunction[Input, Outputs] = {
    case sumstats(phenotype, ancestry, _) => Outputs.Named(s"$phenotype/$ancestry")
    case _ => Outputs.Null
  }

  /** Just need a single machine with no applications, but a good drive. */
  override def cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    applications = Seq.empty,
    masterVolumeSizeInGB = 100,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("install-gc.sh"))),
    stepConcurrency = 4
  )

  override def make(output: String): Job = {
    val flags = output.split("/").toSeq match {
      case Seq(phenotype, ancestry) =>
        Seq(s"--phenotype=$phenotype", s"--ancestry=$ancestry")
    }

    new Job(Job.Script(resourceUri("runDatasetGeneticCorrelation.py"), flags:_*))
  }

}

