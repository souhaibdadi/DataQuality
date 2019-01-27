package fr.edf.dco.edma.edq.output

import fr.edf.dco.edma.edq.input.ConnexionHelper
import fr.edf.dco.edma.edq.job.UnvalidateData
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD

object HbaseOutput extends ConnexionHelper with Output {

  override def save(unvalidateData : RDD[UnvalidateData],table:String): Unit = {
    implicit var hbaseTable : String  = table

    hbaseConfiguration.set(TableOutputFormat.OUTPUT_TABLE, table)
    val job = Job.getInstance(hbaseConfiguration)
    job.setOutputFormatClass(classOf[TableOutputFormat[String]])

    unvalidateData.map(unvalidateDataToPut).saveAsNewAPIHadoopDataset(job.getConfiguration)
  }

  def unvalidateDataToPut(data:UnvalidateData)(implicit table:String) = {
    val put = new Put(Bytes.toBytes(table))
    put.addColumn(Bytes.toBytes("d"),Bytes.toBytes("listOfIds_" + data.checkId + "_" + System.currentTimeMillis() + "_" + data.nbOfRowsUnvalidated),Bytes.toBytes(data.unvalidateRowKey.toString()))
    (put.getRow,put)
  }

}
