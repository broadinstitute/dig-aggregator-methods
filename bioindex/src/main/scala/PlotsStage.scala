package org.broadinstitute.dig.aggregator.methods.bioindex

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

/** The final result of all aggregator methods is building the Bio Index. All
  * outputs are to the dig-bio-index bucket in S3.
  */
class PlotsStage(implicit context: Context) extends Stage {
  val variants   = Input.Source.Dataset("variants/*/*/")
  val bottomLine = Input.Source.Success("out/metaanalysis/trans-ethnic/*/")

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(variants, bottomLine)

  /** Use memory-optimized machine with sizeable disk space for shuffling. */
  override val cluster: ClusterDef = super.cluster.copy(
    masterInstanceType = Ec2.Strategy.memoryOptimized(),
    masterVolumeSizeInGB = 800,
    instances = 1,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("plots/cluster-bootstrap.sh")))
  )

  /** Rules for mapping input to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case variants(dataset, phenotype) => Outputs.Named(s"$dataset/$phenotype")
    case bottomLine(phenotype)        => Outputs.Named(phenotype)
  }

  /** Output to Job steps. */
  override def make(output: String): Job = {
    val pattern = raw"([^/]+)/(.*)".r
    val plot    = resourceUri("plots/plot_associations.py")

    // either a dataset/phenotype or just bottom-line phenotype
    val (srcdir, outdir) = output match {
      case pattern(dataset, phenotype) =>
        (s"variants/${dataset}/${phenotype}", s"dataset/${dataset}/${phenotype}")
      case phenotype =>
        (s"out/metaanalysis/trans-ethnic/${phenotype}", s"bottom-line/${phenotype}")
    }

    new Job(Job.Script(plot, srcdir, outdir))
  }
}
