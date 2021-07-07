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

import nl.knaw.dans.easy.managedeposit.DepositManager._
import nl.knaw.dans.easy.managedeposit.State.{ State, UNKNOWN }
import nl.knaw.dans.lib.error._
import nl.knaw.dans.lib.logging.DebugEnhancedLogging
import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.commons.io.FileUtils
import org.apache.commons.lang.{ BooleanUtils, StringUtils }
import org.joda.time.{ DateTime, DateTimeZone, Duration }
import resource.managed

import java.nio.file.{ Files, Path }
import scala.collection.JavaConverters._
import scala.language.postfixOps
import scala.math.Ordering.{ Long => LongComparator }
import scala.util.{ Failure, Success, Try }
import scala.xml.{ NodeSeq, XML }

class DepositManager(val deposit: Deposit) extends DebugEnhancedLogging {
  private lazy val depositProperties: Option[PropertiesConfiguration] = findDepositProperties
  private lazy val lastModified: Option[DateTime] = getLastModifiedTimestamp.unsafeGetOrThrow

  def properties: Map[String, String] = {
    depositProperties
      .map(props => {
        props.getKeys.asScala
          .map(key => key -> props.getString(key))
          .toMap
      })
      .getOrElse(Map.empty) + ("depositId" -> deposit.getFileName.toString)
  }

  def getNumberOfContinuedDeposits: Int = {
    if (Files.exists(deposit)) {
      deposit.list(_.count(_.getFileName.toString.matches("""^.*\.zip\.\d+$""")))
    }
    else 0
  }

  def getDoiIdentifier: Option[String] = {
    getProperty("identifier.doi")
  }

  def getDansDoiRegistered: Option[String] = {
    getProperty("identifier.dans-doi.registered")
  }

  def getFedoraIdentifier: Option[String] = {
    getProperty("identifier.fedora")
  }

  def getDepositId: Option[String] = {
    getProperty(depositIdKey).orElse(Option(deposit.getFileName).map(_.toString))
  }

  def getStateDescription: Option[String] = {
    getProperty(stateDescription).map(descr => StringUtils.abbreviate(descr, 1000))
  }

  def getDepositorId: Option[String] = {
    getProperty("depositor.userId")
  }

  def getDepositOrigin: Option[String] = {
    getProperty("deposit.origin")
  }

  def getCreationTime: Option[DateTime] = {
    getProperty("creation.timestamp").map(timeString => new DateTime(timeString))
  }

  def isCurationRequired: Boolean = {
    getProperty("curation.required").fold(false)(BooleanUtils.toBoolean)
  }

  def isCurationPerformed: Boolean = {
    getProperty("curation.performed").fold(false)(BooleanUtils.toBoolean)
  }

  def getDatamanager: Option[Datamanager] = {
    getProperty("curation.datamanager.userId")
  }

  def getBagDirName: Option[String] = {
    getProperty("bag-store.bag-name").orElse(retrieveBagNameFromFilesystem)
  }

  private def retrieveBagNameFromFilesystem: Option[String] = {
    managed(Files.list(deposit))
      .map(_.iterator().asScala.toList)
      .acquireAndGet {
        case children if children.count(Files.isDirectory(_)) == 1 =>
          children.find(Files.isDirectory(_)).map(_.getFileName.toString)
        case _ => None
      }
  }

  /**
   * getStateLabel returns the state of the deposit based on its deposit.properties file
   * if the state.label entry is absent or has a unrecognised value State.UNKNOWN is returned
   *
   * @return State enumeration
   */
  def getStateLabel: State = {
    getProperty(stateLabelKey).flatMap(State.toState).getOrElse(UNKNOWN)
  }

  def setState(stateLabel: State): Unit = {
    setProperty(stateLabelKey, stateLabel.toString)
  }

  def setProperties(propertiesMap: (String, String)*): Unit = {
    propertiesMap.foreach { case (key, value) => depositProperties.foreach(_.setProperty(key, value)) }
  }

  def setProperty(key: String, value: String): Unit = {
    depositProperties.foreach(_.setProperty(key, value))
  }

  def saveProperties(): Unit = {
    depositProperties.foreach(_.save(depositPropertiesFileName))
  }

  def hasDepositor(filterOnDepositor: Option[DepositorId]): Boolean = {
    filterOnDepositor.forall(getDepositorId.getOrElse(notAvailable) ==)
  }

  def hasDatamanager(filterOnDatamanager: Option[Datamanager]): Boolean = {
    filterOnDatamanager.forall(getDatamanager.getOrElse(notAvailable) ==)
  }

  def getDepositInformation(location: String)(implicit dansDoiPrefixes: List[String]): Try[DepositInformation] = Try {
    DepositInformation(
      depositId = getDepositId.getOrElse(notAvailable),
      doiIdentifier = getDoi.map(_.getOrElse(notAvailable)).unsafeGetOrThrow,
      dansDoiRegistered = getDansDoiRegistered.map(BooleanUtils.toBoolean),
      fedoraIdentifier = getFedoraIdentifier.getOrElse(notAvailable),
      depositor = getDepositorId.getOrElse(notAvailable),
      state = getStateLabel,
      description = getStateDescription.getOrElse(notAvailable),
      creationTimestamp = getCreationTime.getOrElse(notAvailable).toString,
      numberOfContinuedDeposits = getNumberOfContinuedDeposits,
      storageSpace = FileUtils.sizeOfDirectory(deposit.toFile),
      lastModified = lastModified.map(_.toString(dateTimeFormatter)).getOrElse(notAvailable),
      origin = getDepositOrigin.getOrElse(notAvailable),
      location = location,
      bagDirName = getBagDirName.getOrElse(notAvailable),
      datamanager = getDatamanager.getOrElse(notAvailable),
    )
  }.doIfFailure { case t: Throwable => logger.error(s"[${ deposit.getFileName }] Error while getting depositInformation: ${ t.getMessage }") }

  def depositAgeIsLargerThanRequiredAge(age: Age): Boolean = { // used in delete age check
    val creationTime = getCreationTime
    if (creationTime.isEmpty) logger.warn(s"deposit: $getDepositId does not have a creation time") // a doIfEmpty method would be nice
    creationTime.fold(false)(start => new Duration(start, end).getStandardDays > age)
  }

  def isOlderThan(filterOnAge: Option[Age]): Boolean = { // forall returns true for the empty set, see https://en.wikipedia.org/wiki/Vacuous_truth
    filterOnAge.forall(age => lastModified.forall(mod => Duration.millis(DateTime.now(mod.getZone).getMillis - mod.getMillis).getStandardDays <= age))
  }

  def validateUserCanReadTheDepositDirectoryAndTheDepositProperties(): Try[Unit] = {
    for {
      _ <- validateThatDepositDirectoryIsReadable()
      _ <- validateUserRightsForDepositDir()
      _ <- validateThatDepositPropertiesIsReadable()
      _ <- validateUserRightsForPropertiesFile()
    } yield ()
  }

  /**
   * Returns whether the deposit is valid, also logs a warn if it is not.
   *
   * @return Boolean if the deposit is readable and contains the expected deposit.properties file
   */
  def isValidDeposit: Boolean = {
    validateUserCanReadTheDepositDirectoryAndTheDepositProperties()
      .doIfFailure { case t: Throwable => logger.warn(s"[${ deposit.getFileName }] was invalid: ${ t.getMessage }") }
      .isSuccess
  }

  def getLastModifiedTimestamp: Try[Option[DateTime]] = {
    for {
      _ <- validateUserCanReadTheDepositDirectoryAndTheDepositProperties()
      _ <- validateFilesInDepositDirectoryAreReadable()
    } yield doGetLastModifiedStamp()
  }

  def deleteDepositFromDir(deleteParams: DeleteParameters, location: String)(implicit dansDoiPrefixes: List[String]): Try[Option[DepositInformation]] = {
    for {
      _ <- validateUserCanReadTheDepositDirectoryAndTheDepositProperties()
      shouldDelete = shouldDeleteDepositDir(deleteParams.filterOnDepositor, deleteParams.age, deleteParams.state)
      depositInfo <- if (shouldDelete) deleteDepositFromDirectory(deleteParams, location)
                     else Success(None)
    } yield depositInfo
  }

  private def doGetLastModifiedStamp(): Option[DateTime] = {
    managed(Files.list(deposit)).acquireAndGet {
      _.map[Long](Files.getLastModifiedTime(_).toInstant.toEpochMilli)
        .max(LongComparator)
        .map[DateTime](new DateTime(_, DateTimeZone.UTC))
        .toOption
    }
  }

  private def deleteDepositFromDirectory(deleteParams: DeleteParameters, location: String)(implicit dansDoiPrefixes: List[String]): Try[Option[DepositInformation]] = {
    def doDelete(): Try[Boolean] = {
      val depositorId = getDepositorId
      val depositState = getStateLabel
      if (deleteParams.onlyData)
        deleteOnlyDataFromDeposit(deleteParams.doUpdate, depositorId, depositState)
          .doIfSuccess {
            case true if deleteParams.doUpdate => deleteParams.newState.foreach { case (newStateLabel, newStateDescription) => setState(newStateLabel, newStateDescription) }
            case _ => // do nothing
          }
      else
        deleteDepositDirectory(deleteParams.doUpdate, depositorId, depositState)
    }

    for {
      depositInfo <- getDepositInformation(location)
      deletableFiles <- doDelete()
    } yield if (deletableFiles) Some(depositInfo)
            else None
  }

  private def deleteDepositDirectory(doUpdate: Boolean, depositorId: Option[String], depositState: State): Try[Boolean] = Try {
    if (doUpdate) {
      logger.info(s"DELETE deposit for ${ depositorId.getOrElse("<unknown>") } from $depositState $deposit")
      FileUtils.deleteDirectory(deposit.toFile)
    }
    true
  }

  private def deleteOnlyDataFromDeposit(doUpdate: Boolean, depositorId: Option[DepositorId], depositState: State): Try[Boolean] = Try {
    var filesToDelete = false
    deposit.toFile.listFiles()
      .withFilter(_.getName != depositPropertiesFileName) // don't delete the deposit.properties file
      .map(_.toPath)
      .foreach(path => {
        filesToDelete = true
        validateThatFileIsReadable(path)
          .doIfSuccess(_ => doDeleteDataFromDeposit(doUpdate, depositorId, depositState, path)).unsafeGetOrThrow
      })
    filesToDelete
  }

  private def doDeleteDataFromDeposit(doUpdate: Boolean, depositorId: Option[DepositorId], depositState: State, path: Path): Unit = {
    if (doUpdate) {
      logger.info(s"DELETE data from deposit for ${ depositorId.getOrElse("<unknown>") } from $depositState $deposit")
      if (Files.isDirectory(path)) FileUtils.deleteDirectory(path.toFile)
      else Files.delete(path)
    }
  }

  private def shouldDeleteDepositDir(filterOnDepositor: Option[DepositorId], age: Int, state: State): Boolean = {
    val depositorId: DepositorId = getDepositorId.orNull
    val ageRequirementIsMet = depositAgeIsLargerThanRequiredAge(age)

    // forall returns true for the empty set, see https://en.wikipedia.org/wiki/Vacuous_truth
    filterOnDepositor.forall(depositorId ==) && ageRequirementIsMet && getStateLabel == state
  }

  private def setState(newStateLabel: State.State, newStateDescription: String): Unit = {
    setProperty(stateLabelKey, newStateLabel.toString)
    setProperty(stateDescription, newStateDescription)
    saveProperties()
  }

  private def getProperty(key: String): Option[String] = {
    depositProperties.flatMap(props => Option(props.getString(key)))
  }

  private def end: DateTime = DateTime.now(DateTimeZone.UTC)

  private def depositPropertiesFilePath: Path = deposit.resolve(depositPropertiesFileName)

  private def readDepositProperties(depositDir: Path): PropertiesConfiguration = {
    new PropertiesConfiguration() {
      setDelimiterParsingDisabled(true)
      load(depositDir.resolve(depositPropertiesFileName).toFile)
    }
  }

  private def getDoi: Try[Option[String]] = {
    for {
      _ <- validateUserCanReadTheDepositDirectoryAndTheDepositProperties()
      _ <- validateFilesInDepositDirectoryAreReadable()
      _ <- validateUserRightForMetadataDir()
    } yield getDoiIdentifier.orElse(retrieveDoiFromMetadata)
  }

  private def findDoi(identifiers: NodeSeq): Option[String] = {
    identifiers.find {
      id =>
        id.attribute(XML_NAMESPACE_XSI, "type").exists {
          case Seq(n) =>
            n.text.split(':') match {
              case Array(pre, suffix) => id.getNamespace(pre) == XML_NAMESPACE_ID_TYPE && suffix == "DOI"
              case _ => false
            }
        }
    }.map(_.text)
  }

  private def retrieveDoiFromMetadata: Option[String] = {
    managed(Files.list(deposit)).acquireAndGet {
      _.iterator().asScala.toStream
        .collectFirst {
          case bagDir if Files.isDirectory(bagDir) => bagDir.resolve(metadataDirName).resolve(dataSetFileName)
        }
        .flatMap {
          case datasetXml if Files.exists(datasetXml) => Try {
            val docElement = XML.loadFile(datasetXml.toFile)
            findDoi(docElement \\ "dcmiMetadata" \\ "identifier")
          }.getOrElse(None)
          case _ => None
        }
    }
  }

  private def findDepositProperties: Option[PropertiesConfiguration] = {
    if (Files.exists(depositPropertiesFilePath)) {
      debug(s"Getting info from $deposit")
      Some(readDepositProperties(deposit))
    }
    else {
      logger.error(s"$depositPropertiesFileName does not exist for $deposit")
      None
    }
  }

  private def validateUserRightForMetadataDir(): Try[Unit] = {
    managed(Files.list(deposit)).acquireAndGet {
      _.iterator().asScala.toStream
        .collectFirst {
          case bagDir if Files.isDirectory(bagDir) => validateUserRightsForFile(bagDir.resolve(metadataDirName).resolve(dataSetFileName))
        }
    }.getOrElse(Success(()))
  }

  private def validateThatDepositDirectoryIsReadable(): Try[Unit] = {
    validateThatFileIsReadable(deposit)
  }

  private def validateThatDepositPropertiesIsReadable(): Try[Unit] = {
    validateThatFileIsReadable(depositPropertiesFilePath)
  }

  private def validateUserRightsForPropertiesFile(): Try[Unit] = {
    validateUserRightsForFile(depositPropertiesFilePath)
  }

  private def validateUserRightsForDepositDir(): Try[Unit] = {
    validateUserRightsForFile(deposit)
  }

  private def validateFilesInDepositDirectoryAreReadable(): Try[Unit] = {
    managed(Files.list(deposit)).acquireAndGet {
      _.iterator()
        .asScala
        .map(path => validateThatFileIsReadable(path.toRealPath()))
        .collectFirst { case f @ Failure(_: Exception) => f }
        .getOrElse(Success(()))
    }
  }

  private def validateUserRightsForFile(file: Path): Try[Unit] = Try {
    if (Files.exists(file) && !Files.isReadable(file))
      throw NotReadableException(depositPropertiesFilePath)
  }

  private def validateThatFileIsReadable(path: Path): Try[Unit] = Try {
    if (!Files.isReadable(path))
      throw NotReadableException(path)
  }
}

object DepositManager {
  val depositPropertiesFileName: String = "deposit.properties"
  val depositIdKey = "bag-store.bag-id"
  val stateLabelKey = "state.label"
  val stateDescription = "state.description"
  val metadataDirName = "metadata"
  val dataSetFileName = "dataset.xml"
}
