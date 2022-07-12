package org.broadinstitute.dig.aggregator.methods.bioindex

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

/** The final result of all aggregator methods is building the BioIndex. All
  * outputs are to the dig-bio-index bucket in S3.
  */
class AssociationsStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val bottomLine = Input.Source.Success("out/metaanalysis/trans-ethnic/*/")

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(bottomLine)

  /** Rules for mapping input to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case bottomLine(phenotype) => Outputs.Named(phenotype)
  }

  /** Use memory-optimized machine with sizeable disk space for shuffling. */
  override val cluster: ClusterDef = super.cluster.copy(
    masterInstanceType = Ec2.Strategy.memoryOptimized(mem = 128.gb),
    masterVolumeSizeInGB = 200,
    slaveVolumeSizeInGB = 64,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("cluster-bootstrap.sh")))
  )

  /** Output to Job steps. */
  override def make(output: String): Job = {
    val phenotype = output
    val srcdir    = s"out/metaanalysis/trans-ethnic/$phenotype/"
    val outdir    = s"phenotype/$phenotype/"
    val steps = Seq(
      Job.PySpark(resourceUri("associations.py"), phenotype),
      Job.Script(resourceUri("plotAssociations.py"), srcdir, outdir)
    )

    new Job(steps)
  }
}
