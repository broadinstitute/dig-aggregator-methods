package org.broadinstitute.dig.aggregator.methods.gregor

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws.emr.Job

/** Gathers all the output variants from the trans-ethnic, meta-analysis
  * results and generates a unique list of globally-significany SNPs for use
  * with GREGOR.
  */
class SnpListStage(implicit context: Context) extends Stage {
  val bottomLine: Input.Source = Input.Source.Success("out/metaanalysis/trans-ethnic/*/")

  /** All the processors this processor depends on.
    */
  override val sources: Seq[Input.Source] = Seq(bottomLine)

  /** The output of MetaAnalysis is the phenotype, which is also the output
    * of this processor.
    */
  override val rules: PartialFunction[Input, Outputs] = {
    case bottomLine(phenotype) => Outputs.Named(phenotype)
  }

  /** Find all the unique SNPs from all the output of the meta-analysis processor.
    */
  override def make(output: String): Job = {
    val script    = resourceUri("snplist.py")
    val phenotype = output

    // each phenotype gets its own snp list output
    new Job(Job.PySpark(script, phenotype))
  }
}
