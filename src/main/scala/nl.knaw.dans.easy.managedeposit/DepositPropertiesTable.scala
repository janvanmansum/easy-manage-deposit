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

import nl.knaw.dans.lib.logging.DebugEnhancedLogging
import nl.knaw.dans.lib.error._
import org.apache.commons.configuration.PropertiesConfiguration
import resource.managed

import java.sql.{ Connection, Timestamp }
import scala.collection.mutable
import scala.util.Try
import scala.util.control.NonFatal

class DepositPropertiesTable(database: Database) extends DebugEnhancedLogging {
  private val selectQuery =
    """
      |SELECT
      |  uuid,
      |  last_modified,
      |  properties,
      |  state_label,
      |  depositor_user_id,
      |  datamanager,
      |  storage_size_in_bytes,
      |  location
      |FROM deposit_properties
      |WHERE uuid = ?
      |""".stripMargin

  private val insertQuery =
    """
      |INSERT INTO deposit_properties (
      |  uuid,
      |  last_modified,
      |  properties,
      |  state_label,
      |  depositor_user_id,
      |  datamanager,
      |  storage_size_in_bytes,
      |  location)
      |VALUES (?, ?, ?, ?, ?, ?, ?, ?);
        """.stripMargin

  private val updateQuery =
    """
      |UPDATE deposit_properties
      |SET
      |  last_modified = ?,
      |  properties = ?,
      |  state_label = ?,
      |  depositor_user_id = ?,
      |  datamanager = ?,
      |  storage_size_in_bytes = ?,
      |  location = ?
      |WHERE uuid = ?
        """.stripMargin

  def save(uuid: String, props: PropertiesConfiguration, propsText: String, sizeInBytes: Long, location: String): Try[Unit] = {
    trace(uuid, props, propsText, sizeInBytes, location)
    for {
      optPropString <- database.doTransaction(implicit c => select(uuid))
      _ = debug(s"Found result for $uuid?: ${ optPropString.isDefined }")
      _ <- optPropString
        .map(_ => database.doTransaction(implicit c => update(uuid, props, propsText, sizeInBytes, location)))
        .getOrElse(database.doTransaction(implicit c => insert(uuid, props, propsText, sizeInBytes, location)))
    } yield ()
  }

  def get(uuid: String): Try[Option[String]] = {
    database.doTransaction(implicit c => select(uuid))
  }

  private def select(uuid: String)(implicit c: Connection): Try[Option[String]] = {
    trace(uuid)
    managed(c.prepareStatement(selectQuery))
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

  private def insert(uuid: String, props: PropertiesConfiguration, propsText: String, sizeInBytes: Long, location: String)(implicit c: Connection): Try[Unit] = {
    trace(uuid, props, propsText, sizeInBytes, location)

    managed(c.prepareStatement(insertQuery))
      .map(ps => {
        ps.setString(1, uuid)
        ps.setTimestamp(2, new Timestamp(System.currentTimeMillis()))
        ps.setString(3, propsText)
        ps.setString(4, props.getString("state.label"))
        ps.setString(5, props.getString("depositor.userId"))
        ps.setString(6, props.getString("curation.datamanager.userId", ""))
        ps.setLong(7, sizeInBytes)
        ps.setString(8, location)
        val n = ps.executeUpdate()
        debug(s"inserted $n rows")
      }).tried.map(_ => ()).doIfFailure {
      case NonFatal(e) => logger.error("Could perform insert", e)
    }
  }


  private def update(uuid: String, props: PropertiesConfiguration, propsText: String, sizeInBytes: Long, location: String)(implicit c: Connection): Try[Unit] = {
    trace(uuid, props, propsText, sizeInBytes, location)

    managed(c.prepareStatement(updateQuery))
      .map(ps => {
        ps.setTimestamp(1, new Timestamp(System.currentTimeMillis()))
        ps.setString(2, propsText)
        ps.setString(3, props.getString("state.label"))
        ps.setString(4, props.getString("depositor.userId"))
        ps.setString(5, props.getString("curation.datamanager.userId", ""))
        ps.setLong(6, sizeInBytes)
        ps.setString(7, location)
        ps.setString(8, uuid)
        val n = ps.executeUpdate()
        debug(s"updated $n rows")
      }).tried.map(_ => ()).doIfFailure {
      case NonFatal(e) => logger.error("Could perform update", e)
    }
  }
}
