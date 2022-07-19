package com.raphtory.examples.coho.companiesStream.graphbuilders.officers

import com.raphtory.api.input._
import com.raphtory.examples.coho.companiesStream.rawModel.officers.Officers
import com.raphtory.examples.coho.companiesStream.rawModel.officers.OfficersJsonProtocol.OfficersFormat
import spray.json._

class OfficerToCompanyGraphBuilder extends GraphBuilder[String] {

  override def parseTuple(tuple: String): Unit = {
    try {
      val command = tuple
      val officer = command.parseJson.convertTo[Officers]
    } catch {
      case e: Exception => e.printStackTrace()
    }

  }

  def sendOfficersToPartition(
                             officer: Officers
                             ) = {
    val officerUrl = officer.data.get.links.get.self.get
    val officerID = officerUrl.substring(officerUrl.lastIndexOf('/') + 1)
    val officerName = officer.data.get.name.get
    val officerAddress = officer.data.get.address.get

    val officerAppointmentListGetUrl =
      s"https://api.company-information.service.gov.uk/officers/${officerID}/appointments"

  }

}
