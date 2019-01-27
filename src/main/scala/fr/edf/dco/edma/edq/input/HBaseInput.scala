package fr.edf.dco.edma.edq.input

import fr.edf.dco.edma.edq.dataNode.DataNode
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConversions._

object HBaseInput {


  def getTable(table: String)(implicit sc: SparkContext, conf:Configuration): RDD[DataNode] = {

    conf.set(TableInputFormat.INPUT_TABLE, table)
    // Speculative
    conf.set("spark.speculation", "true")
    conf.set("spark.speculation.interval", "30s")
    conf.set("spark.speculation.multiplier", "4")
    conf.set("spark.speculation.quantile", "0.75")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    // Configurer le scan HBase pour ameliorer les perfs

    // Scan de la table
    var hbaseResults = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

   hbaseResults.map((hbaseResult) => sourceToDataNodes(hbaseResult._2))
  }

  def sourceToDataNodes(result: Result) = {
    val row =  DataNode(Bytes.toString(result.getRow))
    result.listCells().map({ case cell =>
      row.add(Bytes.toString(CellUtil.cloneFamily(cell)) + ":" + Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell)))
    })
    row
  }
}
