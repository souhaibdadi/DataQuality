package fr.edf.dco.edma.edq.input

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.{SparkConf, SparkContext}

trait ConnexionHelper {

  implicit var sparkConf: SparkConf = new SparkConf().setAppName("EdmaDataQuality")
  implicit var sc: SparkContext = new SparkContext(sparkConf)
  implicit var hbaseConfiguration: Configuration = HBaseConfiguration.create()

}
