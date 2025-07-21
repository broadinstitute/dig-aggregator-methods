package org.broadinstitute.dig.aggregator.methods.intake

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.emr.configurations.Spark

class BottomLineConversionTransferStage(implicit context: Context) extends Stage {


  override val cluster: ClusterDef = super.cluster.copy(
    instances = 3,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("open_data_transfer_bootstrap.sh"))),
    stepConcurrency = 2,
    applicationConfigurations = Seq(
      new Spark.Env().usePython3(),
      new Spark.Config()
        .addProperty("spark.sql.adaptive.enabled",        "true")
        .addProperty("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .addProperty("spark.sql.adaptive.skewJoin.enabled", "true")
        .addProperty(" spark.sql.execution.arrow.pyspark.enabled", "true")

    ),

  )

  val transEthnicInputs: Input.Source = Input.Source.Success("out/metaanalysis/bottom-line/trans-ethnic/*/")
  val ancestrySpecificInputs: Input.Source = Input.Source.Success("out/metaanalysis/bottom-line/ancestry-specific/*/*/")

  override val sources: Seq[Input.Source] = Seq(transEthnicInputs, ancestrySpecificInputs)


  override val rules: PartialFunction[Input, Outputs] = {
    case ancestrySpecificInputs(phenotype, ancestry) =>
      val outputPath = s"$phenotype/${ancestry.split("ancestry=").last}"
      Outputs.Named(outputPath)
    case transEthnicInputs(phenotype) =>
      val outputPath = s"$phenotype/Mixed"
      Outputs.Named(outputPath)
  }


  override def make(output: String): Job = {
    val steps = Seq(
      Job.PySpark(resourceUri("bottomLineTransfer.py"), output)
    )

    new Job(steps)
  }
}
