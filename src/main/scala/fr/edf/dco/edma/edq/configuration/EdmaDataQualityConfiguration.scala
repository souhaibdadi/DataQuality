package fr.edf.dco.edma.configuration

import java.util

import com.typesafe.config.Config
import fr.edf.dco.edma.configuration.configuration._
import fr.edf.dco.edma.configuration.checkes._
import fr.edf.dco.edma.edq.configuration.checkes.FieldsChecks
import fr.edf.dco.edma.edq.configuration.configuration.Variable

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

@SerialVersionUID(15L)
class EdmaDataQualityConfiguration extends Serializable {

  private var location: Location = null
  private var fieldsChecks: List[FieldsChecks] = List()

  def getTable() = location.table
  def getType() = location.typ
  def getFieldsChecks() = fieldsChecks

  def this(configuration: Config) {
    this()
    parseLocation(configuration.getConfig("location"))
    parseFieldChecks(configuration.getConfigList("fieldsChecks"))
  }

  private def parseLocation(location: Config) {
    this.location = Location(location.getString("type"), location.getString("table"))
  }

  private def parseFieldChecks(fieldChecks: java.util.List[_ <: Config]): Unit = {
    for (fieldCheck <- fieldChecks) {
      parseChecks(fieldCheck)
    }
  }

  private def parseChecks(checks: Config): Unit = {
    var fieldChecks: FieldsChecks = new FieldsChecks(checks.getString("fieldName"))
    for (check: Config <- checks.getConfigList("checkes")) {
      fieldChecks.addCheck(parseOneCheck(check))
    }
    fieldsChecks = fieldChecks :: fieldsChecks
  }

  private def parseOneCheck(check: Config): Check = {
    check.getString("type") match {
      case Variable.REGEX => Regex(check.getString("id"),"Regex", check.getString("regex"))
      case Variable.REGEXWHERE => parseRegexWhere(check)
      case _ => throw new IllegalArgumentException("Type de regle inconnu")
    }
  }

  private def parseRegexWhere(regexWhere: Config) = {
    val check: RegexWhere = new RegexWhere(regexWhere.getString("id"),regexWhere.getString("regex"))
    for (where: Config <- regexWhere.getConfigList("where")) {
      check.addWhere(Where(where.getString("field"), where.getString("value")))
    }
    check
  }

}

