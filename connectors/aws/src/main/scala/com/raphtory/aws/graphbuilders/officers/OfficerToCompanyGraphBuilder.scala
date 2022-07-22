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


        appointmentList.items.get.foreach { item =>
          if (item.appointed_on.nonEmpty && item.appointed_to.nonEmpty) {
            val companyNumber = item.appointed_to.get.company_number.get
            val resignedOnParsed =
              LocalDate.parse(item.resigned_on.get.replaceAll("\"", ""), DateTimeFormatter.ofPattern("yyyy-MM-dd")).toEpochSecond(LocalTime.MIDNIGHT, ZoneOffset.MIN)
            val appointedOn =
              item.appointed_on.get
            val resignedOn =
              item.resigned_on.get
            val convertedCurrentDate =
              LocalDate.parse(item.appointed_on.get.replaceAll("\"", ""), DateTimeFormatter.ofPattern("yyyy-MM-dd")).toEpochSecond(LocalTime.MIDNIGHT, ZoneOffset.MIN)

            if (appointedOn == resignedOn) {
              addVertex(
                convertedCurrentDate,
                assignID(officerId),
                Properties(ImmutableProperty("name", officerId)),
                Type("Officer ID")
              )
              addVertex(
                convertedCurrentDate,
                assignID(companyNumber),
                Properties(ImmutableProperty("name", companyNumber)),
                Type("Company Number")
              )
              addEdge(
                convertedCurrentDate,
                assignID(officerId),
                assignID(companyNumber),
                Type("Officer to Company")
              )
            }



//            addVertex(
//              convertedCurrentDate,
//              convertedCurrentDate,
//              Properties(ImmutableProperty("name", companyNumber)),
//              Type("appointed on")
//            )
//
//            addVertex(
//              resignedOnParsed,
//              resignedOnParsed,
//              Properties(ImmutableProperty("name", companyNumber)),
//              Type("resigned on")
//            )

//            addEdge(
//              convertedCurrentDate,
//              convertedCurrentDate,
//              resignedOnParsed,
//              Type("Assigned date to resigned on date")
//            )
//
//            addEdge(
//              convertedCurrentDate,
//              assignID(officerId),
//              resignedOnParsed,
//              Type("Officer to resigned on date")
//            )
          }
        }
      }

  }
}
