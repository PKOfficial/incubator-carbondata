package org.apache.streaming.socket

import scala.util.parsing.json.JSON._

object JsonParser {

  def parseJson(jsonString: String): Option[Any] = {
    parseFull(jsonString)
  }

}
