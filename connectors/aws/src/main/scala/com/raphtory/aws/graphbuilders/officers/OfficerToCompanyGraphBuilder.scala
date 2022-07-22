package com.raphtory.aws.graphbuilders.officers

import com.raphtory.api.input._
import com.raphtory.aws.rawModel.officerAppointments.AppointmentListJsonProtocol.OfficerAppointmentListFormat
import com.raphtory.aws.rawModel.officerAppointments.OfficerAppointmentList
import spray.json._
import java.time.{LocalDate, LocalDateTime, LocalTime, ZoneId, ZoneOffset}
import java.time.format.DateTimeFormatter

class OfficerToCompanyGraphBuilder extends GraphBuilder[String] {

  override def parseTuple(tuple: String): Unit = {
    try {
      val command = tuple
      val appointmentList = command.parseJson.convertTo[OfficerAppointmentList]
      sendAppointmentListToPartitions(appointmentList)
    } catch {
      case e: Exception => println("Could not parse appointment")
    }

    def sendAppointmentListToPartitions(
                                         appointmentList: OfficerAppointmentList): Unit = {
      val officerId = appointmentList.links.get.self.get.split("/")(2)
     officerId match {
       case "SBjtBss_I4XEupbfAUXoeAkMcIk" => createVertices()
       case "Yg4rTn5QucYg_hJOxGTnx3B51WY" => createVertices()
       case "C7trUnW0xAvzpaSmVXVviwNi2BY" => createVertices()
       case "8d_bnTiwfxh8JIr3YfuwkmkWkCg" => createVertices()
       case "xLPL0PBzn14BtfuhzOZQswj4AoM" => createVertices()
       case "aDjhOpnMaB_uAHDxRnMLWpa9C-I" => createVertices()
     }

      def createVertices() = {
        appointmentList.items.get.foreach { item =>
          if (item.appointed_on.nonEmpty && item.appointed_to.nonEmpty) {
            val companyNumber = item.appointed_to.get.company_number.get
            val appointedOn = item.appointed_on.get
            val resignedOn = item.resigned_on.get

            val convertedCurrentDate =
              LocalDate.parse(item.appointed_on.get.replaceAll("\"", ""), DateTimeFormatter.ofPattern("yyyy-MM-dd")).toEpochSecond(LocalTime.MIDNIGHT, ZoneOffset.MIN)

            addVertex(
              convertedCurrentDate,
              assignID(officerId),
              Properties(ImmutableProperty("name", officerId)),
              Type("Officer ID")
            )

            addVertex(
              convertedCurrentDate,
              assignID(companyNumber),
              Properties(
                ImmutableProperty("name", companyNumber),
                StringProperty("appointed on", appointedOn),
                StringProperty("resigned on", resignedOn)),
              Type("Company Number")
            )

            addEdge(
              convertedCurrentDate,
              assignID(officerId),
              assignID(companyNumber),
              Type("Company to Officer")
            )
          }
        }
      }
    }
  }
}
