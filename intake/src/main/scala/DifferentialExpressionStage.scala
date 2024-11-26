package org.broadinstitute.dig.aggregator.methods.ldsc

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aws.Ec2.Strategy
import org.broadinstitute.dig.aws.MemorySize

class DifferentialExpressionStage(implicit context: Context) extends Stage {

  val differential_expression: Input.Source = Input.Source.Raw("annotated_regions/gene_expression_levels/*/dataset_metadata")

  /** Source inputs. */
  override val sources: Seq[Input.Source] = Seq(differential_expression)

  /** Map inputs to their outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case differential_expression(dataset) => Outputs.Named(dataset)
  }

  /** Just need a single machine with no applications, but a good drive. */
  override def cluster: ClusterDef = super.cluster.copy(
    instances = 1
  )

  override def make(output: String): Job = {
    new Job(Job.Script(resourceUri("differential_expression.py"), s"--dataset=$output"))
  }
}

