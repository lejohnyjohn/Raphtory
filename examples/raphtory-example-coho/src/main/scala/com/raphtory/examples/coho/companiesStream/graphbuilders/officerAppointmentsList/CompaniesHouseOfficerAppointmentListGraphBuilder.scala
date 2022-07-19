package com.raphtory.examples.coho.companiesStream.graphbuilders.officerAppointmentsList

import com.raphtory.api.input._
import com.raphtory.examples.coho.companiesStream.rawModel.officerAppointments.AppointmentListJsonProtocol.OfficerAppointmentListFormat
import com.raphtory.examples.coho.companiesStream.rawModel.officerAppointments.OfficerAppointmentList
import spray.json._

class CompaniesHouseOfficerAppointmentListGraphBuilder extends GraphBuilder[String] {

  override def parseTuple(tuple: String): Unit = {

    try {
      val command = tuple
      val appointmentList = command.parseJson.convertTo[OfficerAppointmentList]
      sendAppointmentsToPartitions(appointmentList)
    } catch {
      case e: Exception => e.printStackTrace
    }

    def sendAppointmentsToPartitions(appointmentList: OfficerAppointmentList) = {
      val url = appointmentList.links.get.self.get
      val officer_id = url.split("/")(2)
      appointmentList.items.get.foreach { item =>
        val timestamp = item.appointed_on.get.toLong
        addVertex(
          timestamp,
          item.appointed_to.get.company_number.get.hashCode,
          Properties(StringProperty("company_name", item.name.get)),
          Type("Company")
        )
      }
    }


  }

}
