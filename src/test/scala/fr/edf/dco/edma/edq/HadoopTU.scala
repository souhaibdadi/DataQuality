package fr.edf.dco.edma.edq

//import fr.jetoile.hadoopunit.HadoopBootstrap
//import fr.jetoile.hadoopunit.Config
//import fr.jetoile.hadoopunit.Component
import com.typesafe.config.Config
import fr.edf.dco.edma.configuration.{EdmaDataQualityConfiguration, configurationLoader}
import fr.edf.dco.edma.edq.dataNode.{DataNode, Validation}
import fr.edf.dco.edma.edq.helpers.HBaseHelper
import org.apache.commons.configuration.{ConfigurationException, PropertiesConfiguration}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.rdd.RDD
//import org.apache.hadoop.hbase.HBaseConfiguration
//import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.{SparkConf, SparkContext}


object HadoopTU {


  def main(args: Array[String]): Unit = {


    // Lecture de la configuration
    val configs: Seq[Config] = configurationLoader.parse("src/test/ressources")
    val edmaDataQuality = new EdmaDataQualityConfiguration(configs(0))

    // Creer un contexte Spark
    val sparkConf = new SparkConf().setAppName("EdmaDataQuality").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    // Creer un contexte HBase
    val hbaseConfiguration = HBaseConfiguration.create()
    hbaseConfiguration.set("hbase.zookeeper.quorum","127.0.0.1:22010")
    hbaseConfiguration.set("zookeeper.znode.parent","/hbase-unsecure")

    val data: RDD[DataNode] = HBaseHelper.getTable(sc, hbaseConfiguration, edmaDataQuality.getTable())

    val test: RDD[(String, String, String, Boolean)] = data.flatMap(dataNode => Validation.validate(dataNode,edmaDataQuality.getFieldsChecks()))

    test.collect().map(row => println(row))
  }

}
