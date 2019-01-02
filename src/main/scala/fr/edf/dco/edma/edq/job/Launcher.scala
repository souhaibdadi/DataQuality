package fr.edf.dco.edma.edq.job

import com.typesafe.config.Config
import org.apache.spark.{SparkConf, SparkContext}
import fr.edf.dco.edma.configuration.configurationLoader
import fr.edf.dco.edma.configuration.EdmaDataQualityConfiguration
import fr.edf.dco.edma.edq.helpers.HBaseHelper
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.rdd.RDD
import fr.edf.dco.edma.edq.dataNode.Validation
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
class Launcher {

  val sparkConf = new SparkConf().setAppName("EdmaDataQuality")
  val sc = new SparkContext(sparkConf)
  val hbaseConfiguration: Configuration = HBaseConfiguration.create()

  def run(): Unit = {
    val configs: Seq[Config] = configurationLoader.parse(".")
    configs.map(conf => launchTest(conf))
  }

  private def launchTest(conf: Config): Unit = {

    val edmaDataQuality = new EdmaDataQualityConfiguration(conf)
    val result = edmaDataQuality.getType() match {
      case "HBase" => HBaseHelper.getTable(sc, hbaseConfiguration, edmaDataQuality.getTable())
    }

    result.flatMap(dataNode => Validation.validate(dataNode,edmaDataQuality.getFieldsChecks()) )

  }

}
