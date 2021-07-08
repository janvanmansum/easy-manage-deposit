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

import nl.knaw.dans.easy.managedeposit.Command.FeedBackMessage
import nl.knaw.dans.lib.error._
import nl.knaw.dans.lib.logging.DebugEnhancedLogging
import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.commons.io.FileUtils
import resource.managed

import java.io.StringReader
import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.{ Files, Path }
import scala.collection.JavaConverters._
import scala.language.postfixOps
import scala.util.{ Failure, Success, Try }

class EasyManageDepositApp(configuration: Configuration) extends DebugEnhancedLogging with Curation {
  override val fedora: Fedora = configuration.fedora
  override val landingPageBaseUrl: URI = configuration.landingPageBaseUrl

  private val propsTable = {
    val database = new Database(
      url = configuration.databaseUrl,
      user = configuration.databaseUser,
      password = configuration.databasePassword,
      driver = configuration.databaseDriver)
    logger.info("Initializing database connection...")
    database.initConnectionPool()
      .doIfSuccess { _ => logger.info("Database connection initialized.") }
      .doIfFailure { case e: Throwable => throw new IllegalStateException("Cannot connect to database", e) }
    new DepositPropertiesTable(database)
  }

  def saveDepositProperties(uuid: String, props: PropertiesConfiguration, propsText: String, lastModified: Long, sizeInBytes: Long, location: String): Try[Unit] = {
    trace(uuid, props, propsText)
    propsTable.save(uuid, props, propsText, lastModified, sizeInBytes, location)
  }

  def getDepositProperties(uuid: String): Try[Option[String]] = {
    trace(uuid)
    propsTable.get(uuid)
  }

  def loadDepositDirectoriesFromAllLocations(): Try[String] = Try {
    trace(())
    (configuration.sword2DepositsDir :: configuration.ingestFlowInbox :: Nil)
      .foreach(loadDepositDirectoriesFromParent)
    configuration.ingestFlowInboxArchived.foreach(loadDepositDirectoriesFromParent)
    "Loaded deposits from all locations"
  }

  def loadDepositDirectoriesFromLocation(location: String): Try[String] = Try {
    trace(location)
    location match {
      case "SWORD2" => loadDepositDirectoriesFromParent(configuration.sword2DepositsDir)
      case "INGEST_FLOW" => loadDepositDirectoriesFromParent(configuration.ingestFlowInbox)
      case "INGEST_FLOW_ARCHIVED" => configuration.ingestFlowInboxArchived.map(loadDepositDirectoriesFromParent).getOrElse(throw new IllegalStateException("No INGEST_FLOW_ARCHIVED configured"))
      case _ => throw new IllegalArgumentException(s"Unkown location $location")
    }
    s"Loaded deposits from $location"
  }

  def loadDepositDirectoriesFromParent(parent: Path): Try[Unit] = Try {
    trace(parent)
    managed(Files.newDirectoryStream(parent))
      .map(_.asScala
        .toStream
        .withFilter(p => Files.isDirectory(p))
        .map(loadSingleDepositDir)
        .toList)
      .tried.foreach(_ => logger.info(s"Loaded deposits in $parent"))
  }

  def loadSingleDepositProperties(uuid: String): Try[String] = {
    trace(uuid)
    for {
      optDepositDir <- findDepositDir(uuid)
      _ = debug(s"Deposit = ${ optDepositDir }")
      _ <- optDepositDir.map(loadSingleDepositDir).getOrElse(Failure(new IllegalArgumentException(s"No such deposit: $uuid")))
    } yield s"Loaded deposit $uuid into database"
  }

  private def findDepositDir(uuid: String): Try[Option[Path]] = Try {
    (configuration.sword2DepositsDir #:: configuration.ingestFlowInbox #:: configuration.ingestFlowInboxArchived.toStream)
      .map(_.resolve(uuid))
      .collectFirst { case path if Files.exists(path) => path }
  }

  def loadSingleDepositDir(dir: Path): Try[Unit] = Try {
    trace(dir)
    if (Files.exists(dir)) {
      val propsFile = dir.resolve("deposit.properties")
      val uuid = dir.getFileName.toString
      val propsString = FileUtils.readFileToString(propsFile.toFile, StandardCharsets.UTF_8)
      val lastModified = Files.getLastModifiedTime(propsFile).toMillis
      val sizeInBytes = FileUtils.sizeOfDirectory(dir.toFile)
      val location = getLocationFromPath(dir).getOrElse("UNKOWN")
      readDepositProperties(propsString)
        .flatMap(p => saveDepositProperties(uuid, p, propsString, lastModified, sizeInBytes, location))
        .map(_ => logger.info(s"Loaded $uuid"))
        .unsafeGetOrThrow
    }
    else {
      throw new IllegalArgumentException(s"No such deposit $dir")
    }
  }

  private def getLocationFromPath(path: Path): Option[String] = {
    val ap = path.toAbsolutePath
    if (ap.startsWith(configuration.sword2DepositsDir.toAbsolutePath)) Some("SWORD2")
    else if (ap.startsWith(configuration.ingestFlowInbox.toAbsolutePath)) Some("INGEST_FLOW")
         else if (configuration.ingestFlowInboxArchived.exists(ifa => ap.startsWith(ifa.toAbsolutePath))) Some("INGEST_FLOW_ARCHIVED")
              else None
  }

  def readDepositProperties(s: String): Try[PropertiesConfiguration] = Try {
    trace(s)
    new PropertiesConfiguration() {
      setDelimiterParsingDisabled(true)
      load(new StringReader(s))
      checkMandatoryKey(this, "state.label")
      checkMandatoryKey(this, "state.description")
      checkMandatoryKey(this, "depositor.userId")
    }
  }

  private def checkMandatoryKey(props: PropertiesConfiguration, key: String): Unit = {
    if (!props.containsKey(key)) throw new IllegalArgumentException(s"Missing mandatory key: '$key'")
  }

  private implicit val dansDoiPrefixes: List[String] = configuration.dansDoiPrefixes

  private def collectDataFromDepositsDir(depositsDir: Path, filterOnDepositor: Option[DepositorId], filterOnDatamanager: Option[Datamanager], filterOnAge: Option[Age], location: String): Deposits = {
    depositsDir.list(collectDataFromDepositsDir(filterOnDepositor, filterOnDatamanager, filterOnAge, location))
  }

  private def collectRawDepositProperties(depositsDir: Path): Seq[Seq[String]] = {
    depositsDir.list(collectRawDepositProperties)
  }

  def deleteDepositsFromDepositsDir(depositsDir: Path, deleteParams: DeleteParameters, location: String): Try[Deposits] = {
    depositsDir.list(deleteDepositsFromDepositsDir(deleteParams, location))
  }

  private def collectDataFromDepositsDir(filterOnDepositor: Option[DepositorId], filterOnDatamanager: Option[Datamanager], filterOnAge: Option[Age], location: String)(depositPaths: List[Path]): Deposits = {
    trace(filterOnDepositor)
    getDepositManagers(depositPaths)
      .withFilter(_.isValidDeposit)
      .withFilter(_.hasDepositor(filterOnDepositor))
      .withFilter(_.hasDatamanager(filterOnDatamanager))
      .withFilter(_.isOlderThan(filterOnAge))
      .map(_.getDepositInformation(location))
      .collect { case Success(d: DepositInformation) => d }
  }

  private def collectRawDepositProperties(depositPaths: List[Path]): Seq[Seq[String]] = {
    makeCompleteTable(getDepositManagers(depositPaths).map(_.properties))
  }

  /**
   * Given a sequence of maps of key-value pairs, construct a table that has values for every key in every map.
   *
   * @example
   * {{{
   *    [
   *      { "a" -> "1", "b" -> "2", "c" -> "3" },
   *      { "a" -> "4", "c" -> "5" },
   *      { "b" -> "6", "c" -> "7", "d" -> "8" },
   *    ]
   *
   *    should result in
   *
   *    [
   *      [ "a",   "b",   "c", "d"   ],
   *      [ "1",   "2",   "3", "n/a" ],
   *      [ "4",   "n/a", "5", "n/a" ],
   *      [ "n/a", "6",   "7", "8"   ],
   *    ]
   * }}}
   * @param input        the sequence of maps to be made into a complete table
   * @param defaultValue the value (lazily evaluated) to be used for values that are not available (defaults to `"n/a"`)
   * @return the completed table
   */
  private def makeCompleteTable(input: Seq[Map[String, String]], defaultValue: => String = "n/a"): Seq[Seq[String]] = {
    val keys: List[String] = input.flatMap(_.keys.toSeq).distinct.toList

    keys +: input.map(m => keys.map(m.getOrElse(_, defaultValue)))
  }

  private def getDepositManagers(depositPaths: List[Path]): List[DepositManager] = {
    depositPaths.collect { case file if Files.isDirectory(file) => new DepositManager(file) }
  }

  def deleteDepositsFromDepositsDir(deleteParams: DeleteParameters, location: String)(depositPaths: List[Path]): Try[List[DepositInformation]] = Try {
    for {
      depositManager <- getDepositManagers(depositPaths)
      depositInformation <- depositManager.deleteDepositFromDir(deleteParams, location)
        .doIfFailure {
          case e: Exception => logger.error(s"[${ depositManager.getDepositId }] Error while deleting deposit: ${ e.getMessage }", e)
        }
        .unsafeGetOrThrow
    } yield depositInformation
  }

  def summary(depositor: Option[DepositorId], datamanager: Option[Datamanager], age: Option[Age]): Try[String] = Try {
    val sword2Deposits = collectDataFromDepositsDir(configuration.sword2DepositsDir, depositor, datamanager, age, "SWORD2")
    val ingestFlowDeposits = collectDataFromDepositsDir(configuration.ingestFlowInbox, depositor, datamanager, age, "INGEST_FLOW")
    val ingestFlowArchivedDeposits = configuration.ingestFlowInboxArchived.map(collectDataFromDepositsDir(_, depositor, datamanager, age, "INGEST_FLOW_ARCHIVED")).getOrElse(Seq.empty)
    ReportGenerator.outputSummary(sword2Deposits ++ ingestFlowDeposits ++ ingestFlowArchivedDeposits, depositor)(Console.out)
    "End of summary report."
  }

  def createFullReport(depositor: Option[DepositorId], datamanager: Option[Datamanager], age: Option[Age]): Try[String] = Try {
    val sword2Deposits = collectDataFromDepositsDir(configuration.sword2DepositsDir, depositor, datamanager, age, "SWORD2")
    val ingestFlowDeposits = collectDataFromDepositsDir(configuration.ingestFlowInbox, depositor, datamanager, age, "INGEST_FLOW")
    val ingestFlowArchivedDeposits = configuration.ingestFlowInboxArchived.map(collectDataFromDepositsDir(_, depositor, datamanager, age, "INGEST_FLOW_ARCHIVED")).getOrElse(Seq.empty)
    ReportGenerator.outputFullReport(sword2Deposits ++ ingestFlowDeposits ++ ingestFlowArchivedDeposits)(Console.out)
    "End of full report."
  }

  def createErrorReport(depositor: Option[DepositorId], datamanager: Option[Datamanager], age: Option[Age]): Try[String] = Try {
    val sword2Deposits = collectDataFromDepositsDir(configuration.sword2DepositsDir, depositor, datamanager, age, "SWORD2")
    val ingestFlowDeposits = collectDataFromDepositsDir(configuration.ingestFlowInbox, depositor, datamanager, age, "INGEST_FLOW")
    val ingestFlowArchivedDeposits = configuration.ingestFlowInboxArchived.map(collectDataFromDepositsDir(_, depositor, datamanager, age, "INGEST_FLOW_ARCHIVED")).getOrElse(Seq.empty)
    ReportGenerator.outputErrorReport(sword2Deposits ++ ingestFlowDeposits ++ ingestFlowArchivedDeposits)(Console.out)
    "End of error report."
  }

  def createRawReport(location: Path): Try[String] = Try {
    ReportGenerator.outputRawReport(collectRawDepositProperties(location))(Console.out)
    "End of raw report."
  }

  def cleanDeposits(deleteParams: DeleteParameters): Try[FeedBackMessage] = {
    for {
      sword2DeletedDeposits <- deleteDepositsFromDepositsDir(configuration.sword2DepositsDir, deleteParams, "SWORD2")
      ingestFlowDeletedDeposits <- deleteDepositsFromDepositsDir(configuration.ingestFlowInbox, deleteParams, "INGEST_FLOW")
    } yield {
      if (deleteParams.output || !deleteParams.doUpdate)
        ReportGenerator.outputDeletedDeposits(sword2DeletedDeposits ++ ingestFlowDeletedDeposits)(Console.out)
      "Execution of clean: success "
    }
  }

  def syncFedoraState(easyDatasetId: DatasetId): Try[FeedBackMessage] = {
    for {
      _ <- validateUserCanReadAllDepositsInIngestFlowBox()
      manager <- findDepositManagerForDatasetId(easyDatasetId)
      curationMessage <- curate(manager)
    } yield curationMessage
  }

  private def validateUserCanReadAllDepositsInIngestFlowBox(): Try[Unit] = {
    val deposits = Files.newDirectoryStream(configuration.ingestFlowInbox).asScala.toList
    getDepositManagers(deposits)
      .map(_.validateUserCanReadTheDepositDirectoryAndTheDepositProperties())
      .collectFirst { case f @ Failure(_: Exception) => f }
      .getOrElse(Success(()))
  }

  private def findDepositManagerForDatasetId(easyDatasetId: DatasetId): Try[DepositManager] = Try {
    configuration.ingestFlowInbox
      .list(_.collect { case deposit if Files.isDirectory(deposit) => new DepositManager(deposit) })
      .collectFirst { case manager if manager.getFedoraIdentifier.contains(easyDatasetId) => manager }
      .getOrElse(throw new IllegalArgumentException(s"No deposit found for datatsetId $easyDatasetId"))
  }
}
