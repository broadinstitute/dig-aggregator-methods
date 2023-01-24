package org.broadinstitute.dig.aggregator.methods.ldsc

class AnnotationToLDStage(implicit context: Context) extends Stage {

  val partitions: Seq[String] = Seq()
  val subRegion: String = if (partitions.isEmpty) "default" else partitions.mkString("-")
  val annotationFiles: Input.Source = Input.Source.Success(s"out/ldsc/regions/${subRegion}/annot/*/*/")

  /** Source inputs. */
  override val sources: Seq[Input.Source] = Seq(annotationFiles)

  /** Just need a single machine with no applications, but a good drive. */
  override def cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    applications = Seq.empty,
    bootstrapScripts = Seq(new BootstrapScript(resourceUri("install-ldscore.sh"))),
    releaseLabel = ReleaseLabel("emr-6.7.0")
  )

  override val rules: PartialFunction[Input, Outputs] = {
    case annotationFiles(annotation, ancestry) => Outputs.Named(s"$annotation/${ancestry.split("=").last}")
  }

  override def make(output: String): Job = {
    new Job(Job.Script(resourceUri("regionsToLD.py"), s"--sub-region=$subRegion", s"--annotation-path=$output"))
  }

  /** Update the success flag of the merged regions.
   */
  override def success(output: String): Unit = {
    context.s3.touch(s"out/ldsc/regions/${subRegion}/ld_score/$output/_SUCCESS")
    ()
  }
}
