package fr.edf.dco.edma.edq.dataNode

import fr.edf.dco.edma.configuration.checkes.{Check, Regex, RegexWhere, Where}
import fr.edf.dco.edma.edq.configuration.checkes.FieldsChecks

import scala.util.control.Breaks._
import scala.util.matching

case class checkResult(rowKey:String,qualifier:String,id:String,isChecked:Boolean)

object Validation {

  def validate(rowData:DataNode,listOfFieldChecks:List[FieldsChecks]): List[checkResult] = {
    listOfFieldChecks.flatMap(fieldsChecks => processFieldChecks(rowData,fieldsChecks))
  }

  def processFieldChecks(rowData: DataNode, fieldChecks:FieldsChecks): List[checkResult] = {
    val field = fieldChecks.getField()
    fieldChecks.checks.map(check => checksFactory(rowData,check,field))
  }


  def checksFactory(rowData:DataNode, check:Check, field:String): checkResult = {
    check match {
      case regex : Regex => checkRegex(rowData,regex,field)
      case regexWhere : RegexWhere => checkRegexWhere(rowData,regexWhere,field)   // TODO
      //case _ =>
    }
  }

  def checkRegex(rowData:DataNode, check:Regex, field:String) : checkResult = {
    rowData.isExist(field) match {
      case true => checkResult(rowData.rowKey,field,check.id,rowData.getField(field).get.matches(check.regex))
      case false => checkResult(rowData.rowKey,field,check.id,false) // TODO : Prendre en compte l'information champs mandatory
    }
  }

  def checkRegexWhere(rowData:DataNode, check:RegexWhere, field:String) : checkResult = {
    rowData.isExist(field) match {
      case true => checkResult(rowData.rowKey,field,check.getId,rowData.getField(field).get.matches(check.getRegex) && checkWhere(rowData,check.wheres))
      case false => checkResult(rowData.rowKey,field,check.getId,false)
    }
  }

  def checkWhere (rowData:DataNode,wheres:List[Where]) : Boolean = {
    var conditions = false
    breakable {
      for(where <- wheres){
        conditions = rowData.isExist(where.field) && rowData.getField(where.field).get.contentEquals(where.value)
        if (conditions == true) break()
      }
    }
    conditions
  }

}
