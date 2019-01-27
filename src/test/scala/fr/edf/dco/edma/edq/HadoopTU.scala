package fr.edf.dco.edma.edq

//import fr.jetoile.hadoopunit.HadoopBootstrap
//import fr.jetoile.hadoopunit.Config
//import fr.jetoile.hadoopunit.Component
import com.typesafe.config.Config
import fr.edf.dco.edma.configuration.{EdmaDataQualityConfiguration, configurationLoader}
import fr.edf.dco.edma.edq.dataNode.{DataNode, Validation, checkResult}
import fr.edf.dco.edma.edq.input.HBaseInput
import fr.edf.dco.edma.edq.job.{Launcher, UnvalidateData}
import fr.edf.dco.edma.edq.job.Launcher._
import org.apache.commons.configuration.{ConfigurationException, PropertiesConfiguration}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.mapreduce.Job
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
    implicit val sparkConf = new SparkConf().setAppName("EdmaDataQuality").setMaster("local[2]")
    implicit val sc = new SparkContext(sparkConf)
    // Creer un contexte HBase
    implicit val hbaseConfiguration = HBaseConfiguration.create()
    hbaseConfiguration.set("hbase.zookeeper.quorum","127.0.0.1:22010")
    hbaseConfiguration.set("zookeeper.znode.parent","/hbase-unsecure")


    // SAVE
    hbaseConfiguration.set(TableOutputFormat.OUTPUT_TABLE, "dco_edma:Utilisateur")
    val job = Job.getInstance(hbaseConfiguration)
    job.setOutputFormatClass(classOf[TableOutputFormat[String]])



    val data: RDD[DataNode] = HBaseInput.getTable(edmaDataQuality.getTable())

    val test: RDD[checkResult] = data.flatMap(dataNode => Validation.validate(dataNode,edmaDataQuality.getFieldsChecks()))

    val unvalidateData: RDD[UnvalidateData] = collectUnvalidateData(test)

    val puts: RDD[(Array[Byte], Put)] = saveUnvalidateData(unvalidateData,edmaDataQuality.getTable())


    puts.saveAsNewAPIHadoopDataset(job.getConfiguration)
  }

}
