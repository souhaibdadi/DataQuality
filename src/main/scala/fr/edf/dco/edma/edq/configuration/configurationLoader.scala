package fr.edf.dco.edma.configuration
import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
object configurationLoader {

  def parse(path:String): Seq[Config] = {
    new File(path).listFiles().filter(ifConfig).map(file => ConfigFactory.parseFile(file))
  }

  def ifConfig(file:File) =  file.isFile && file.getName().startsWith("edq")
}
