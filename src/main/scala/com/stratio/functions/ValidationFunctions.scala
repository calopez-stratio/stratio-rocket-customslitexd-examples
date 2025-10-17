package com.stratio.functions

class ValidationFunctions {

  def getRequiredProperty(propertyName: String, customProperties: Map[String, String]): String = {
    customProperties.getOrElse(propertyName,
      throw new NoSuchElementException(s"Required property '$propertyName' not found"))
  }
}
