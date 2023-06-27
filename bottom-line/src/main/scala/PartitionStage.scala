package org.broadinstitute.dig.aggregator.methods.bottomline

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy

class PartitionStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val variants: Input.Source = Input.Source.Success("furkan-v1-new/variants/*/*/*/")

  // The small cluster is cheapest (even for large datasets) generally
  // For speed on a large phenotype with lots of Mixed datasets a c5.9xlarge is a cost efficient option
  // TODO: Reassess if/when the file structure on the variants directory allows the selection of only Mixed datasets
  override val cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("cluster-bootstrap.sh"))),
    releaseLabel = ReleaseLabel("emr-6.7.0") // Need emr 6.1+ to read zstd files
  )

  override val sources: Seq[Input.Source] = Seq(variants)

  override val rules: PartialFunction[Input, Outputs] = {
    case variants(_, dataset, phenotype) => Outputs.Named(PartitionOutput(dataset, phenotype).toOutput)
  }

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
