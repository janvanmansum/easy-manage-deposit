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

import com.yourmediashelf.fedora.client.{ FedoraClient, FedoraCredentials }
import org.apache.commons.configuration.PropertiesConfiguration
import resource.managed
import nl.knaw.dans.lib.string._
import scala.collection.JavaConverters._

import java.net.{ URI, URL }
import java.nio.file.{ Files, Path, Paths }
import scala.io.Source

case class Configuration(version: String, properties: PropertiesConfiguration)

case class Configuration2(version: String,
                          serverPort: Int,
                          databaseUrl: URI,
                          databaseUser: String,
                          databasePassword: String,
                          databaseDriver: String,
                          fedora: Fedora,
                          sword2DepositsDir: Path,
                          ingestFlowInbox: Path,
                          ingestFlowInboxArchived: Option[Path],
                          landingPageBaseUrl: URI,
                          dansDoiPrefixes: List[String])


object Configuration {

  def apply(home: Path): Configuration = {
    val cfgPath = Seq(
      Paths.get(s"/etc/opt/dans.knaw.nl/easy-manage-deposit/"),
      home.resolve("cfg"))
      .find(Files.exists(_))
      .getOrElse { throw new IllegalStateException("No configuration directory found") }

    new Configuration(
      version = managed(Source.fromFile(home.resolve("bin/version").toFile)).acquireAndGet(_.mkString),
      properties = new PropertiesConfiguration(cfgPath.resolve("application.properties").toFile)
    )
  }

  def apply2(home: Path): Configuration2 = {
    val cfgPath = Seq(
      Paths.get(s"/etc/opt/dans.knaw.nl/easy-manage-deposit/"),
      home.resolve("cfg"))
      .find(Files.exists(_))
      .getOrElse { throw new IllegalStateException("No configuration directory found") }
    val properties = new PropertiesConfiguration() {
      setDelimiterParsingDisabled(true)
      load(cfgPath.resolve("application.properties").toFile)
    }
    val fedoraCredentials = new FedoraCredentials(
      new URL(properties.getString("fedora.url")),
      properties.getString("fedora.user"),
      properties.getString("fedora.password"))

    Configuration2(
      version = managed(Source.fromFile(home.resolve("bin/version").toFile)).acquireAndGet(_.mkString),
      serverPort = properties.getInt("daemon.http.port"),
      databaseUrl = new URI(properties.getString("database.url")),
      databaseUser = properties.getString("database.user"),
      databasePassword = properties.getString("database.password"),
      databaseDriver = properties.getString("database.driver"),
      fedora = new Fedora(new FedoraClient(fedoraCredentials)),
      sword2DepositsDir = Paths.get(properties.getString("easy-sword2")),
      ingestFlowInbox = Paths.get(properties.getString("easy-ingest-flow-inbox")),
      ingestFlowInboxArchived = properties.getString("easy-ingest-flow-inbox-archived").toOption.map(Paths.get(_)).filter(Files.exists(_)),
      landingPageBaseUrl = new URI(properties.getString("landing-pages.base-url")),
      dansDoiPrefixes = properties.getList("dans-doi.prefixes")
      .asScala.toList
      .map(prefix => prefix.asInstanceOf[String]))
  }
}
