package fr.edf.dco.edma.edq.output

import fr.edf.dco.edma.edq.job.UnvalidateData
import org.apache.spark.rdd.RDD

trait Output {
  def save(unvalidateData : RDD[UnvalidateData],table:String) : Unit
}
