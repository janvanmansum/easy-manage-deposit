/**
 * Copyright (C) 2017 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.knaw.dans.easy.managedeposit

import nl.knaw.dans.easy.managedeposit.State.State
import nl.knaw.dans.lib.error._
import nl.knaw.dans.lib.logging.DebugEnhancedLogging
import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.commons.lang.BooleanUtils
import org.joda.time.format.{ DateTimeFormatter, ISODateTimeFormat }
import org.joda.time.{ DateTime, DateTimeZone }
import resource.managed

import java.io.StringReader
import java.sql.{ Connection, Timestamp }
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Try
import scala.util.control.NonFatal

class DepositPropertiesTable(database: Database)(implicit val dansDoiPrefixes: List[String]) extends DebugEnhancedLogging {
  private val dateTimeFormatter: DateTimeFormatter = ISODateTimeFormat.dateTime()

  def save(uuid: String, props: PropertiesConfiguration, propsText: String, location: String, lastModified: Long, hasBag: Boolean, sizeInBytes: Long, numberOfContinuedDeposits: Int): Try[Unit] = {
    trace(uuid, props, propsText, sizeInBytes, location)
    for {
      optPropString <- database.doTransaction(implicit c => selectUuid(uuid))
      _ = debug(s"Found result for $uuid?: ${ optPropString.isDefined }")
      _ <- optPropString
        .map(_ => database.doTransaction(implicit c => update(uuid, props, propsText, location, lastModified, hasBag, sizeInBytes, numberOfContinuedDeposits)))
        .getOrElse(database.doTransaction(implicit c => insert(uuid, props, propsText, location, lastModified, hasBag, sizeInBytes, numberOfContinuedDeposits)))
    } yield ()
  }

  def get(uuid: String): Try[Option[String]] = {
    database.doTransaction(implicit c => selectUuid(uuid))
  }

  def deleteProperties(optLocation: Option[String], optUuid: Option[String]): Try[Unit] = {
    database.doTransaction(implicit c => {
      if (optLocation.isDefined) deleteLocation(optLocation.get)
      else if (optUuid.isDefined) deleteUUid(optUuid.get)
           else deleteAll()
    })
  }

  def processDepositInformation(filterStates: List[State] = List.empty,
                                filterDepositor: Option[String] = None,
                                filterDatamanager: Option[String] = None,
                                filterAgeInDays: Option[Int] = None)(processor: DepositInformation => Unit): Try[Unit] = {
    trace(())
    database.doTransaction(implicit c => doWithReportInformation(filterStates, filterDepositor, filterDatamanager, filterAgeInDays)(processor))
  }

  private def doWithReportInformation(filterStates: List[State] = List.empty,
                                      filterDepositor: Option[String] = None,
                                      filterDatamanager: Option[String] = None,
                                      filterAgeInDays: Option[Int] = None)
                                     (action: DepositInformation => Unit)(implicit c: Connection): Try[Unit] = Try {
    trace(())
    val whereConditions = new ListBuffer[String]()
    if (filterStates.nonEmpty) whereConditions.append("state_label in (" + filterStates.map(s => s"'$s'").mkString(",") + ")")
    if (filterDepositor.nonEmpty) whereConditions.append(s"depositor_user_id = '${filterDepositor.get}'")
    if (filterDatamanager.nonEmpty) whereConditions.append(s"datamanager = '${filterDatamanager.get}'")
    if (filterAgeInDays.nonEmpty) whereConditions.append(s"last_modified > NOW() - INTERVAL '${filterAgeInDays.get} days'")


    val query =
      s"""
         |SELECT
         |  uuid,
         |  last_modified,
         |  properties,
         |  state_label,
         |  depositor_user_id,
         |  datamanager,
         |  location,
         |  has_bag,
         |  storage_size_in_bytes,
         |  number_of_continued_deposits
         |FROM deposit_properties
         |${
        if (whereConditions.isEmpty) ""
        else "WHERE " + whereConditions.mkString(" AND\n")}
         |ORDER BY last_modified
         |""".stripMargin

    debug(s"query = $query")

    managed(c.prepareStatement(query))
      .map(ps => {
        val resultSet = ps.executeQuery()
        while (resultSet.next()) {
          val propsText = resultSet.getString("properties")
          val props = new PropertiesConfiguration() {
            setDelimiterParsingDisabled(true)
            load(new StringReader(propsText))
          }

          action(
            DepositInformation(
              depositId = resultSet.getString("uuid"),
              doiIdentifier = props.getString("identifier.doi", "n/a"),
              dansDoiRegistered = Option(BooleanUtils.toBoolean(props.getString("identifier.dans-doi.registered"))),
              fedoraIdentifier = props.getString("identifier.fedora", "n/a"),
              depositor = resultSet.getString("depositor_user_id"),
              state = State.toState(resultSet.getString("state_label")).getOrElse(State.UNKNOWN),
              description = props.getString("state.description", "n/a"),
              creationTimestamp = props.getString("creation.timestamp", "n/a"),
              numberOfContinuedDeposits = resultSet.getInt("number_of_continued_deposits"),
              storageSpace = resultSet.getLong("storage_size_in_bytes"),
              lastModified = new DateTime(resultSet.getTimestamp("last_modified")).withZone(DateTimeZone.UTC).toString(dateTimeFormatter),
              origin = props.getString("deposit.origin", "n/a"),
              location = resultSet.getString("location"),
              bagDirName = props.getString("bag-store.bag-name", "n/a"),
              datamanager = resultSet.getString("datamanager"),
              hasBag = resultSet.getBoolean("has_bag")
            ))
        }
      }).tried.doIfFailure {
      case NonFatal(e) => logger.error("Error while processing deposit information", e)
    }
  }

  private def selectUuid(uuid: String)(implicit c: Connection): Try[Option[String]] = {
    trace(uuid)

    val selectUuidQuery =
      """
        |SELECT
        |  uuid,
        |  last_modified,
        |  properties,
        |  state_label,
        |  depositor_user_id,
        |  datamanager,
        |  location,
        |  has_bag,
        |  storage_size_in_bytes,
        |  number_of_continued_deposits
        |FROM deposit_properties
        |WHERE uuid = ?
        |""".stripMargin

    managed(c.prepareStatement(selectUuidQuery))
      .map(ps => {
        ps.setString(1, uuid)
        ps.executeQuery()
      }).map(
      r => {
        val ss = new mutable.ListBuffer[String]()

        while (r.next()) {
          ss.append(r.getString("properties"))
        }
        if (ss.size > 1) throw new IllegalStateException(s"Found ${ ss.size } deposits with the same UUID")
        ss.headOption
      }).tried.doIfFailure {
      case NonFatal(e) => logger.error("Could perform select", e)
    }
  }

  private def insert(uuid: String, props: PropertiesConfiguration, propsText: String, location: String, lastModified: Long, hasBag: Boolean, sizeInBytes: Long, numberOfContinuedDeposits: Int)(implicit c: Connection): Try[Unit] = {
    trace(uuid, props, propsText, sizeInBytes, location)

    val insertQuery =
      """
        |INSERT INTO deposit_properties (
        |  uuid,
        |  last_modified,
        |  properties,
        |  state_label,
        |  depositor_user_id,
        |  datamanager,
        |  location,
        |  has_bag,
        |  storage_size_in_bytes,
        |  number_of_continued_deposits)
        |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """.stripMargin

    managed(c.prepareStatement(insertQuery))
      .map(ps => {
        ps.setString(1, uuid)
        ps.setTimestamp(2, new Timestamp(lastModified))
        ps.setString(3, propsText)
        ps.setString(4, props.getString("state.label"))
        ps.setString(5, props.getString("depositor.userId"))
        ps.setString(6, props.getString("curation.datamanager.userId", ""))
        ps.setString(7, location)
        ps.setBoolean(8, hasBag)
        ps.setLong(9, sizeInBytes)
        ps.setInt(10, numberOfContinuedDeposits)

        val n = ps.executeUpdate()
        debug(s"inserted $n rows")
      }).tried.map(_ => ()).doIfFailure {
      case NonFatal(e) => logger.error("Could not perform insert", e)
    }
  }

  private def update(uuid: String, props: PropertiesConfiguration, propsText: String, location: String, lastModified: Long, hasBag: Boolean, sizeInBytes: Long, numberOfContinuedDeposits: Int)(implicit c: Connection): Try[Unit] = {
    trace(uuid, props, propsText, sizeInBytes, location)

    val updateQuery =
      """
        |UPDATE deposit_properties
        |SET
        |  last_modified = ?,
        |  properties = ?,
        |  state_label = ?,
        |  depositor_user_id = ?,
        |  datamanager = ?,
        |  location = ?,
        |  has_bag = ?,
        |  storage_size_in_bytes = ?,
        |  number_of_continued_deposits = ?
        |WHERE uuid = ?
        """.stripMargin

    managed(c.prepareStatement(updateQuery))
      .map(ps => {
        ps.setTimestamp(1, new Timestamp(lastModified))
        ps.setString(2, propsText)
        ps.setString(3, props.getString("state.label"))
        ps.setString(4, props.getString("depositor.userId"))
        ps.setString(5, props.getString("curation.datamanager.userId", ""))
        ps.setString(6, location)
        ps.setBoolean(7, hasBag)
        ps.setLong(8, sizeInBytes)
        ps.setInt(9, numberOfContinuedDeposits)
        ps.setString(10, uuid)
        val n = ps.executeUpdate()
        debug(s"updated $n rows")
      }).tried.map(_ => ()).doIfFailure {
      case NonFatal(e) => logger.error("Could not perform update", e)
    }
  }

  private def deleteLocation(location: String)(implicit c: Connection): Try[Unit] = {
    trace(location)

    val deleteLocationQuery =
      """
        |DELETE FROM deposit_properties
        |WHERE location = ?
        |""".stripMargin

    managed(c.prepareStatement(deleteLocationQuery))
      .map(ps => {
        ps.setString(1, location)
        val n = ps.executeUpdate()
        debug(s"deleted $n rows")
      }).tried.map(_ => ()).doIfFailure {
      case NonFatal(e) => logger.error(s"Could not perform delete on location $location", e)
    }
  }

  private def deleteUUid(uuid: String)(implicit c: Connection): Try[Unit] = {
    trace(uuid)

    val deleteUuidQuery =
      """
        |DELETE FROM deposit_properties
        |WHERE uuid = ?
        |""".stripMargin

    managed(c.prepareStatement(deleteUuidQuery))
      .map(ps => {
        ps.setString(1, uuid)
        val n = ps.executeUpdate()
        debug(s"deleted $n rows")
      }).tried.map(_ => ()).doIfFailure {
      case NonFatal(e) => logger.error(s"Could not perform delete on uuid $uuid", e)
    }
  }

  private def deleteAll()(implicit c: Connection): Try[Unit] = {
    trace(())

    val deleteAllQuery =
      """
        |DELETE FROM deposit_properties
        |""".stripMargin

    managed(c.prepareStatement(deleteAllQuery))
      .map(ps => {
        val n = ps.executeUpdate()
        debug(s"deleted $n rows")
      }).tried.map(_ => ()).doIfFailure {
      case NonFatal(e) => logger.error(s"Could not perform delete on all", e)
    }
  }
}
