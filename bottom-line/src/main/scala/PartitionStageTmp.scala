package org.broadinstitute.dig.aggregator.methods.bottomline

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy

class PartitionStageTmp(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val variants: Input.Source = Input.Source.Success("variants/*/*/*/")

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 3,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("cluster-bootstrap.sh"))),
    releaseLabel = ReleaseLabel("emr-6.7.0") // Need emr 6.1+ to read zstd files
  )

  /** Look for new datasets. */
  override val sources: Seq[Input.Source] = Seq(variants)

  /** There is an output made for each phenotype. */
  override val rules: PartialFunction[Input, Outputs] = {
    case variants(_, dataset, phenotype) => Outputs.Named(PartitionOutput(dataset, phenotype).toOutput)
  }

  /** First partition all the variants across datasets (by dataset), then
   * run the ancestry-specific analysis and load it from staging. Finally,
   * run the trans-ethnic analysis and load the resulst from staging.
   */
  override def make(output: String): Job = {
    val partition = resourceUri("partitionVariants.py")
    val partitionOutput = PartitionOutput.fromOutput(output)

    new Job(Seq(Job.PySpark(partition, partitionOutput.dataset, partitionOutput.phenotype)))
  }
}

case class PartitionOutput(
  dataset: String,
  phenotype: String
) {
  def toOutput: String = s"$dataset/$phenotype"
}

object PartitionOutput {
  def fromOutput(output: String): PartitionOutput = output.split("/").toSeq match {
    case Seq(dataset, phenotype) => PartitionOutput(dataset, phenotype)
    case _ => throw new Exception("Invalid Partition Output String")
  }
}
