package fr.edf.dco.edma.edq

import com.typesafe.config.Config
import fr.edf.dco.edma.configuration.{EdmaDataQualityConfiguration, configurationLoader}
import fr.edf.dco.edma.edq.dataNode.{DataNode, Validation}
import fr.edf.dco.edma.edq.input.HBaseInput
import org.apache.commons.configuration.{ConfigurationException, PropertiesConfiguration}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD

import scala.collection.immutable
//import org.apache.hadoop.hbase.HBaseConfiguration
//import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.{SparkConf, SparkContext}

import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat

import scala.collection.JavaConversions._

object HbaseDataInsertion {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("EdmaDataQuality").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    // Creer un contexte HBase
    val hbaseConfiguration = HBaseConfiguration.create()
    hbaseConfiguration.set("hbase.zookeeper.quorum","127.0.0.1:22010")
    hbaseConfiguration.set("zookeeper.znode.parent","/hbase-unsecure")

    hbaseConfiguration.set(TableOutputFormat.OUTPUT_TABLE, "dco_edma:Utilisateur")
    // Speculative
    hbaseConfiguration.set("spark.speculation", "true")
    hbaseConfiguration.set("spark.speculation.interval", "30s")
    hbaseConfiguration.set("spark.speculation.multiplier", "4")
    hbaseConfiguration.set("spark.speculation.quantile", "0.75")
    hbaseConfiguration.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")


    val job = Job.getInstance(hbaseConfiguration)
    job.setOutputFormatClass(classOf[TableOutputFormat[String]])


    // Configurer le scan HBase pour ameliorer les perfs

    // Scan de la table
    val puts: immutable.Seq[(Array[Byte], (Array[Byte], Array[Byte], Array[Byte]))] = for(i <- 1 to 1000) yield ( (Bytes.toBytes(i.toString)), (Bytes.toBytes("d"),Bytes.toBytes("tGerance"),Bytes.toBytes("Cloe")))


    val rdd: RDD[(Array[Byte], Put)] = sc.parallelize(puts).map(data => (data._1,new Put(data._1).addColumn(data._2._1,data._2._2,data._2._3)))
    rdd.saveAsNewAPIHadoopDataset(job.getConfiguration)

  }
}
