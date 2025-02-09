package com.raphtory.examples.coho.companiesStream.graphbuilders.streamapidata

import com.raphtory.api.input._
import com.raphtory.examples.coho.companiesStream.jsonparsers.company.CompaniesHouseJsonProtocol.CompanyFormat
import com.raphtory.examples.coho.companiesStream.jsonparsers.company.Company
import spray.json._

import java.text.SimpleDateFormat
import java.util.Date

/**
  * The CompaniesStreamRawGraphBuilder sets each json object as a vertex
  * and edges as the links between these objects. This creates a big network
  * for a companies information.
  */

class CompaniesStreamRawGraphBuilder extends GraphBuilder[String] {
  private val nullStr = "null"

  def apply(graph: Graph, tuple: String): Unit = {
    try {
      val command = tuple
      val company = command.parseJson.convertTo[Company]
      sendCompanyToPartitions(graph, company)
    }
    catch {
      case e: Exception => e.printStackTrace
    }

    def getTimestamp(dateString: String): Long = {
      val dateFormat = new SimpleDateFormat("yyyy/MM/dd")
      var date: Date = new Date()
      try date = dateFormat.parse(dateString)
      catch {
        case e: java.text.ParseException => 0
      }
      date.getTime
    }

    def sendCompanyToPartitions(graph: Graph, company: Company): Unit = {
      val companyNumber = company.resource_id.get.hashCode()
      val timeFromCoho  = company.data.get.date_of_creation.get
      val timestamp     = getTimestamp(timeFromCoho)
      graph.addVertex(
              timestamp,
              companyNumber,
              Properties(
                      MutableString(
                              "resource_kind",
                              company.resource_kind match {
                                case Some(kind) => kind
                                case None       => nullStr
                              }
                      ),
                      MutableString(
                              "resource_uri",
                              company.resource_uri match {
                                case Some(uri) => uri
                                case None      => nullStr
                              }
                      ),
                      MutableString(
                              "resource_id",
                              company.resource_id match {
                                case Some(id) => id
                                case None     => nullStr
                              }
                      ),
                      MutableString(
                              "data",
                              company.data match {
                                case Some(data) => data.company_number.toString
                                case None       => nullStr
                              }
                      ),
                      MutableInteger(
                              "event",
                              company.event match {
                                case Some(event) => event.timepoint.get
                                case None        => 0
                              }
                      )
              )
      )

      // CompanyProfile resource data
      for (data <- company.data) {
        val dataCompanyNumber = data.company_number.get.hashCode()
        graph.addVertex(
                timestamp,
                companyNumber,
                Properties(
                        MutableBoolean(
                                "can_file",
                                data.can_file match {
                                  case Some(can_file) => can_file
                                  case None           => false
                                }
                        ),
                        MutableString(
                                "company_name",
                                data.company_name match {
                                  case Some(company_name) => company_name
                                  case None               => nullStr
                                }
                        ),
                        MutableString(
                                "company_number",
                                data.company_number match {
                                  case Some(company_number) => company_number
                                  case None                 => nullStr
                                }
                        ),
                        MutableString(
                                "company_status",
                                data.company_status match {
                                  case Some(company_status) => company_status
                                  case None                 => nullStr
                                }
                        ),
                        MutableString(
                                "company_status_detail",
                                data.company_status_detail match {
                                  case Some(company_status_detail) => company_status_detail
                                  case None                        => nullStr
                                }
                        ),
                        MutableString(
                                "date_of_cessation",
                                data.date_of_cessation match {
                                  case Some(date_of_cessation) => date_of_cessation
                                  case None                    => nullStr
                                }
                        ),
                        MutableString("date_of_creation", data.date_of_creation.get),
                        MutableString(
                                "etag",
                                data.etag match {
                                  case Some(etag) => etag
                                  case None       => nullStr
                                }
                        ),
                        MutableBoolean(
                                "has_been_liquidated",
                                data.has_been_liquidated match {
                                  case Some(has_been_liquidated) => has_been_liquidated
                                  case None                      => false
                                }
                        ),
                        MutableBoolean(
                                "has_charges",
                                data.has_charges match {
                                  case Some(has_charges) => has_charges
                                  case None              => false
                                }
                        ),
                        MutableBoolean(
                                "has_insolvency_history",
                                data.has_insolvency_history match {
                                  case Some(has_insolvency_history) => has_insolvency_history
                                  case None                         => false
                                }
                        ),
                        MutableBoolean(
                                "is_community_interest_company",
                                data.is_community_interest_company match {
                                  case Some(is_community_interest_company) => is_community_interest_company
                                  case None                                => false
                                }
                        ),
                        MutableString("jurisdiction", data.jurisdiction.get),
                        MutableString(
                                "last_full_members_list_date",
                                data.last_full_members_list_date match {
                                  case Some(last_full_members_list_date) => last_full_members_list_date
                                  case None                              => nullStr
                                }
                        ),
                        MutableBoolean(
                                "registered_office_is_in_dispute",
                                data.registered_office_is_in_dispute match {
                                  case Some(registered_office_is_in_dispute) => registered_office_is_in_dispute
                                  case None                                  => false
                                }
                        ),
                        MutableString(
                                "sic_codes",
                                data.sic_codes match {
                                  case Some(sic_codes) => sic_codes
                                  case None            => nullStr
                                }
                        ),
                        MutableBoolean(
                                "undeliverable_registered_office_address",
                                data.undeliverable_registered_office_address match {
                                  case Some(undeliverable_registered_office_address) =>
                                    undeliverable_registered_office_address
                                  case None                                          => false
                                }
                        ),
                        MutableString(
                                "type",
                                data.`type` match {
                                  case Some(_type) => _type
                                  case None        => nullStr
                                }
                        )
                )
        )
        //Edge from company to companyProfile resource data
        graph.addEdge(timestamp, companyNumber, dataCompanyNumber, Properties(MutableString("type", "companyToData")))

        //Company Accounts Information
        for (account <- data.accounts) {
          //The Accounting Reference Date (ARD) of the company
          for (accountingReference <- account.accounting_reference_date)
            graph.addVertex(
                    timestamp,
                    companyNumber,
                    Properties(
                            MutableString(
                                    "accounting_reference_date_day",
                                    accountingReference.day match {
                                      case Some(day) => day
                                      case None      => nullStr
                                    }
                            ),
                            MutableString(
                                    "accounting_reference_date_month",
                                    accountingReference.month match {
                                      case Some(month) => month
                                      case None        => nullStr
                                    }
                            )
                    )
            )

          //The last company accounts filed
          for (lastAccounts <- account.last_accounts)
            graph.addVertex(
                    timestamp,
                    companyNumber,
                    Properties(
                            MutableString(
                                    "last_accounts_made_up_to",
                                    lastAccounts.made_up_to match {
                                      case Some(made_up_to) => made_up_to
                                      case None             => nullStr
                                    }
                            ),
                            MutableString(
                                    "last_accounts_type",
                                    lastAccounts.`type` match {
                                      case Some(_type) => _type
                                      case None        => nullStr
                                    }
                            )
                    )
            )

          //Dates of next company accounts due/made up to + if company accounts overdue
          graph.addVertex(
                  timestamp,
                  companyNumber,
                  Properties(
                          MutableString(
                                  "next_due",
                                  account.next_due match {
                                    case Some(next_due) => next_due
                                    case None           => nullStr
                                  }
                          ),
                          MutableString("next_made_up_to", account.next_made_up_to.get),
                          MutableBoolean(
                                  "overdue",
                                  account.overdue match {
                                    case Some(overdue) => overdue
                                    case None          => false
                                  }
                          )
                  )
          )
          //Edge from data to accounts
          val dst = account.accounting_reference_date.get.day.get
            .hashCode() + account.accounting_reference_date.get.month.get.hashCode()
          graph
            .addEdge(timestamp, dataCompanyNumber, dst, Properties(MutableString("type", "dataToAccountsInformation")))
        }

        //Annual return information
        for (annualReturn <- data.annual_return) {
          graph.addVertex(
                  timestamp,
                  companyNumber,
                  Properties(
                          MutableString(
                                  "last_made_up_to",
                                  annualReturn.last_made_up_to match {
                                    case Some(last_made_up_to) => last_made_up_to
                                    case None                  => nullStr
                                  }
                          ),
                          MutableString(
                                  "next_due",
                                  annualReturn.next_due match {
                                    case Some(next_due) => next_due
                                    case None           => nullStr
                                  }
                          ),
                          MutableString(
                                  "next_made_up_to",
                                  annualReturn.next_made_up_to match {
                                    case Some(next_made_up_to) => next_made_up_to
                                    case None                  => nullStr
                                  }
                          ),
                          MutableBoolean(
                                  "overdue",
                                  annualReturn.overdue match {
                                    case Some(overdue) => overdue
                                    case None          => false
                                  }
                          )
                  )
          )
          val dst = annualReturn.next_due.hashCode()
          //Edge from data to annual return
          graph.addEdge(timestamp, dataCompanyNumber, dst, Properties(MutableString("type", "dataToAnnualReturn")))
        }

        //UK branch of a foreign company
        for (branchCompanyDetails <- data.branch_company_details) {
          graph.addVertex(
                  timestamp,
                  companyNumber,
                  Properties(
                          MutableString(
                                  "business_activity",
                                  branchCompanyDetails.business_activity match {
                                    case Some(business_activity) => business_activity
                                    case None                    => nullStr
                                  }
                          ),
                          MutableString(
                                  "parent_company_name",
                                  branchCompanyDetails.parent_company_name match {
                                    case Some(parent_company_name) => parent_company_name
                                    case None                      => nullStr
                                  }
                          ),
                          MutableString(
                                  "parent_company_number",
                                  branchCompanyDetails.parent_company_number match {
                                    case Some(parent_company_number) => parent_company_number
                                    case None                        => nullStr
                                  }
                          )
                  )
          )
          val dst = branchCompanyDetails.parent_company_number.get.hashCode()
          //Edge from data to branch company details
          graph.addEdge(
                  timestamp,
                  dataCompanyNumber,
                  dst,
                  Properties(MutableString("type", "dataToBranchCompanyDetails"))
          )
        }

        //Confirmation Statement
        for (confirmationStatement <- data.confirmation_statement) {
          graph.addVertex(
                  timestamp,
                  companyNumber,
                  Properties(
                          MutableString(
                                  "last_made_up_to",
                                  confirmationStatement.last_made_up_to match {
                                    case Some(last_made_up_to) => last_made_up_to
                                    case None                  => nullStr
                                  }
                          ),
                          MutableString(
                                  "next_due",
                                  confirmationStatement.next_due match {
                                    case Some(next_due) => next_due
                                    case None           => nullStr
                                  }
                          ),
                          MutableString(
                                  "next_made_up_to",
                                  confirmationStatement.next_made_up_to match {
                                    case Some(next_made_up_to) => next_made_up_to
                                    case None                  => nullStr
                                  }
                          ),
                          MutableBoolean(
                                  "overdue",
                                  confirmationStatement.overdue match {
                                    case Some(overdue) => overdue
                                    case None          => false
                                  }
                          )
                  )
          )
          val dst = confirmationStatement.next_due.get.hashCode()
          //Edge from data to confirmation statement information
          graph.addEdge(
                  timestamp,
                  dataCompanyNumber,
                  dst,
                  Properties(MutableString("type", "dataToConfirmationStatement"))
          )
        }

        //Foreign Company Details
        for (foreignCompanyDetails <- data.foreign_company_details) {
          graph.addVertex(
                  timestamp,
                  companyNumber,
                  Properties(
                          MutableString(
                                  "business_activity",
                                  foreignCompanyDetails.business_activity match {
                                    case Some(business_activity) => business_activity
                                    case None                    => nullStr
                                  }
                          ),
                          MutableString(
                                  "company_type",
                                  foreignCompanyDetails.company_type match {
                                    case Some(company_type) => company_type
                                    case None               => nullStr
                                  }
                          ),
                          MutableString(
                                  "governed_by",
                                  foreignCompanyDetails.governed_by match {
                                    case Some(governed_by) => governed_by
                                    case None              => nullStr
                                  }
                          ),
                          MutableBoolean(
                                  "is_a_credit_finance_institution",
                                  foreignCompanyDetails.is_a_credit_finance_institution match {
                                    case Some(is_a_credit_finance_institution) => is_a_credit_finance_institution
                                    case None                                  => false
                                  }
                          ),
                          MutableString(
                                  "registration_number",
                                  foreignCompanyDetails.registration_number match {
                                    case Some(registration_number) => registration_number
                                    case None                      => nullStr
                                  }
                          )
                  )
          )
          val dst = foreignCompanyDetails.registration_number.get.hashCode()
          //Edge from data to foreign company details
          graph.addEdge(
                  timestamp,
                  dataCompanyNumber,
                  dst,
                  Properties(MutableString("type", "dataToForeignCompanyDetails"))
          )

          //Accounts Requirement
          for (accountingRequirement <- foreignCompanyDetails.accounting_requirement) {
            graph.addVertex(
                    timestamp,
                    companyNumber,
                    Properties(
                            MutableString(
                                    "foreign_account_type",
                                    accountingRequirement.foreign_account_type match {
                                      case Some(foreign_account_type) => foreign_account_type
                                      case None                       => nullStr
                                    }
                            ),
                            MutableString(
                                    "terms_of_account_publication",
                                    accountingRequirement.terms_of_account_publication match {
                                      case Some(terms_of_account_publication) => terms_of_account_publication
                                      case None                               => nullStr
                                    }
                            )
                    )
            )
            val src = foreignCompanyDetails.registration_number.get.hashCode()
            val dst = accountingRequirement.terms_of_account_publication.get.hashCode()
            //Edge from data to accounting requirement
            graph.addEdge(
                    timestamp,
                    src,
                    dst,
                    Properties(MutableString("type", "foreignCompanyDetailsToAccountingRequirement"))
            )
          }

          //Foreign company account information
          for (accounts <- foreignCompanyDetails.accounts) {
            val accountDst = accounts.hashCode()
            graph.addEdge(
                    timestamp,
                    foreignCompanyDetails.registration_number.get.hashCode(),
                    accountDst,
                    Properties(MutableString("type", "foreignCompanyDetailsToAccounts"))
            )
            //Date account period starts under parent law
            for (accountFrom <- accounts.account_period_from) {
              graph.addVertex(
                      timestamp,
                      companyNumber,
                      Properties(
                              MutableString(
                                      "account_period_from_day",
                                      accountFrom.day match {
                                        case Some(day) => day
                                        case None      => nullStr
                                      }
                              ),
                              MutableString(
                                      "account_period_from_month",
                                      accountFrom.month match {
                                        case Some(month) => month
                                        case None        => nullStr
                                      }
                              )
                      )
              )
              graph.addEdge(
                      timestamp,
                      accountDst,
                      accountFrom.day.get.hashCode(),
                      Properties(MutableString("type", "accountsToAccountPeriodFrom"))
              )
            }

            //Date account period ends under parent law
            for (accountTo <- accounts.account_period_to) {
              graph.addVertex(
                      timestamp,
                      companyNumber,
                      Properties(
                              MutableString(
                                      "account_period_to_day",
                                      accountTo.day match {
                                        case Some(day) => day
                                        case None      => nullStr
                                      }
                              ),
                              MutableString(
                                      "account_period_to_month",
                                      accountTo.month match {
                                        case Some(month) => month
                                        case None        => nullStr
                                      }
                              )
                      )
              )
              graph.addEdge(
                      timestamp,
                      accountDst,
                      accountTo.day.get.hashCode(),
                      Properties(MutableString("type", "accountsToAccountPeriodTo"))
              )
            }

            //Time allowed from period end for disclosure of accounts under parent law
            for (mustFileWithin <- accounts.must_file_within) {
              graph.addVertex(
                      timestamp,
                      companyNumber,
                      Properties(
                              MutableString(
                                      "must_file_within_months",
                                      mustFileWithin.months match {
                                        case Some(months) => months
                                        case None         => nullStr
                                      }
                              )
                      )
              )
              graph.addEdge(
                      timestamp,
                      accountDst,
                      mustFileWithin.months.get.hashCode(),
                      Properties(MutableString("type", "accountsToMustFileWithin"))
              )
            }
          }

          //Company origin informations
          for (originatingRegistry <- foreignCompanyDetails.originating_registry) {
            graph.addVertex(
                    timestamp,
                    companyNumber,
                    Properties(
                            MutableString(
                                    "originating_registry_name",
                                    originatingRegistry.name match {
                                      case Some(name) => name
                                      case None       => nullStr
                                    }
                            ),
                            MutableString(
                                    "originating_registry_country",
                                    originatingRegistry.country match {
                                      case Some(country) => country
                                      case None          => nullStr
                                    }
                            )
                    )
            )
            graph.addEdge(
                    timestamp,
                    originatingRegistry.name.get.hashCode(),
                    dst,
                    Properties(MutableString("type", "foreignCompanyDetailsToOriginatingRegistry"))
            )
          }
        }

        // A set of URLs related to the resource, including self
        for (links <- data.links) {
          graph.addVertex(
                  timestamp,
                  companyNumber,
                  Properties(
                          MutableString(
                                  "persons_with_significant_control",
                                  links.persons_with_significant_control match {
                                    case Some(person) => person
                                    case None         => nullStr
                                  }
                          ),
                          MutableString(
                                  "persons_with_significant_control_statement",
                                  links.persons_with_significant_control_statements match {
                                    case Some(person) => person
                                    case None         => nullStr
                                  }
                          ),
                          MutableString(
                                  "registers",
                                  links.registers match {
                                    case Some(registers) => registers
                                    case None            => nullStr
                                  }
                          ),
                          MutableString(
                                  "self",
                                  links.self match {
                                    case Some(self) => self
                                    case None       => nullStr
                                  }
                          )
                  )
          )
          graph.addEdge(
                  timestamp,
                  dataCompanyNumber,
                  links.self.get.hashCode(),
                  Properties(MutableString("type", "dataToLinks"))
          )
        }

        //The previous names of this company
        for (previousCompanyNames    <- data.previous_company_names)
          for (name <- previousCompanyNames) {
            graph.addVertex(
                    timestamp,
                    companyNumber,
                    Properties(
                            MutableString(
                                    "ceased_on",
                                    name.ceased_on match {
                                      case Some(ceased_on) => ceased_on
                                      case None            => nullStr
                                    }
                            ),
                            MutableString(
                                    "effective_from",
                                    name.effective_from match {
                                      case Some(effective_from) => effective_from
                                      case None                 => nullStr
                                    }
                            ),
                            MutableString(
                                    "name",
                                    name.name match {
                                      case Some(name) => name
                                      case None       => nullStr
                                    }
                            )
                    )
            )
            graph.addEdge(
                    timestamp,
                    dataCompanyNumber,
                    name.name.getOrElse(nullStr).hashCode,
                    Properties(MutableString("type", "dataToPreviousNames"))
            )
          }
        //The address of the company's registered office
        for (registeredOfficeAddress <- data.registered_office_address) {
          graph.addVertex(
                  timestamp,
                  companyNumber,
                  Properties(
                          MutableString(
                                  "address_line_1",
                                  registeredOfficeAddress.address_line_1 match {
                                    case Some(address_line) => address_line
                                    case None               => nullStr
                                  }
                          ),
                          MutableString(
                                  "address_line_2",
                                  registeredOfficeAddress.address_line_2 match {
                                    case Some(address_line_2) => address_line_2
                                    case None                 => nullStr
                                  }
                          ),
                          MutableString(
                                  "care_of",
                                  registeredOfficeAddress.care_of match {
                                    case Some(care_of) => care_of
                                    case None          => nullStr
                                  }
                          ),
                          MutableString(
                                  "country",
                                  registeredOfficeAddress.country match {
                                    case Some(country) => country
                                    case None          => nullStr
                                  }
                          ),
                          MutableString(
                                  "locality",
                                  registeredOfficeAddress.locality match {
                                    case Some(locality) => locality
                                    case None           => nullStr
                                  }
                          ),
                          MutableString(
                                  "po_box",
                                  registeredOfficeAddress.po_box match {
                                    case Some(po_box) => po_box
                                    case None         => nullStr
                                  }
                          ),
                          MutableString(
                                  "postal_code",
                                  registeredOfficeAddress.postal_code match {
                                    case Some(postal_code) => postal_code
                                    case None              => nullStr
                                  }
                          ),
                          MutableString(
                                  "premises",
                                  registeredOfficeAddress.premises match {
                                    case Some(premises) => premises
                                    case None           => nullStr
                                  }
                          ),
                          MutableString(
                                  "region",
                                  registeredOfficeAddress.region match {
                                    case Some(region) => region
                                    case None         => nullStr
                                  }
                          )
                  )
          )
          graph.addEdge(
                  timestamp,
                  dataCompanyNumber,
                  registeredOfficeAddress.postal_code.get.hashCode,
                  Properties(MutableString("type", "dataToRegisteredAddress"))
          )
        }

        for (serviceAddress <- data.service_address) {
          graph.addVertex(
                  timestamp,
                  companyNumber,
                  Properties(
                          MutableString(
                                  "address_line_1",
                                  serviceAddress.address_line_1 match {
                                    case Some(address_line_1) => address_line_1
                                    case None                 => nullStr
                                  }
                          ),
                          MutableString(
                                  "address_line_2",
                                  serviceAddress.address_line_2 match {
                                    case Some(address_line_2) => address_line_2
                                    case None                 => nullStr
                                  }
                          ),
                          MutableString(
                                  "care_of",
                                  serviceAddress.care_of match {
                                    case Some(care_of) => care_of
                                    case None          => nullStr
                                  }
                          ),
                          MutableString(
                                  "country",
                                  serviceAddress.country match {
                                    case Some(country) => country
                                    case None          => nullStr
                                  }
                          ),
                          MutableString(
                                  "locality",
                                  serviceAddress.locality match {
                                    case Some(locality) => locality
                                    case None           => nullStr
                                  }
                          ),
                          MutableString(
                                  "po_box",
                                  serviceAddress.po_box match {
                                    case Some(po_box) => po_box
                                    case None         => nullStr
                                  }
                          ),
                          MutableString(
                                  "postal_code",
                                  serviceAddress.postal_code match {
                                    case Some(postal_code) => postal_code
                                    case None              => nullStr
                                  }
                          ),
                          MutableString(
                                  "region",
                                  serviceAddress.region match {
                                    case Some(region) => region
                                    case None         => nullStr
                                  }
                          )
                  )
          )
          graph.addEdge(
                  timestamp,
                  dataCompanyNumber,
                  serviceAddress.postal_code.get.hashCode(),
                  Properties(MutableString("type", "dataToServiceAddress"))
          )
        }

      }

      //Link to the related resource
      for (event <- company.event) {
        graph.addVertex(
                timestamp,
                companyNumber,
                Properties(
                        MutableString(
                                "fields_changed",
                                event.fields_changed match {
                                  case Some(fields_changed) => fields_changed.mkString
                                  case None                 => nullStr
                                }
                        ),
                        MutableInteger(
                                "timepoint",
                                event.timepoint match {
                                  case Some(timepoint) => timepoint
                                  case None            => 0
                                }
                        ),
                        MutableString(
                                "published_at",
                                event.published_at match {
                                  case Some(published_at) => published_at
                                  case None               => nullStr
                                }
                        ),
                        MutableString(
                                "_type",
                                event.`type` match {
                                  case Some(_type) => _type
                                  case None        => nullStr
                                }
                        )
                )
        )
        graph.addEdge(
                timestamp,
                companyNumber,
                event.published_at.get.hashCode(),
                Properties(MutableString("type", "companyToEvent"))
        )
      }
    }
  }
}
