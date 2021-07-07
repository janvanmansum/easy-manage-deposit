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
import nl.knaw.dans.easy.managedeposit.FedoraState.FedoraState
import nl.knaw.dans.easy.managedeposit.State._
import nl.knaw.dans.lib.logging.DebugEnhancedLogging

import java.net.URI
import scala.util.{ Failure, Success, Try }

trait Curation extends DebugEnhancedLogging {
  val fedora: Fedora
  val landingPageBaseUrl: URI

  def curate(manager: DepositManager): Try[FeedBackMessage] = {
    val depositId = manager.getDepositId.getOrElse(manager.deposit.getFileName.toString)
    manager.getFedoraIdentifier
      .map(curate(manager, depositId))
      .getOrElse(Failure(new IllegalStateException(s"[${ manager.getDepositId }] no datasetId found during curation")))
  }

  private def curate(manager: DepositManager, depositId: DepositId)(id: DatasetId): Try[FeedBackMessage] = {
    manager.getStateLabel match {
      case State.IN_REVIEW => getFedoraStateAndUpdateProperties(manager, id, depositId)
      case state => logAndReturnMessage(s"[$depositId] deposit with datasetId $id has state $state, no action required")
    }
  }

  private def getFedoraStateAndUpdateProperties(manager: DepositManager, datasetId: DatasetId, depositId: DepositId): Try[FeedBackMessage] = {
    fetchAmdAndExtractState(datasetId, depositId).flatMap {
      case FedoraState.PUBLISHED =>
        manager.setProperties(
          "identifier.dans-doi.registered" -> "yes",
          "curation.performed" -> "yes",
          "state.label" -> FEDORA_ARCHIVED.toString,
          "state.description" -> s"${ landingPageBaseUrl.resolve(s"./$datasetId") }"
        )
        manager.saveProperties()
        logAndReturnMessage(s"[$depositId] deposit with datasetId $datasetId has been successfully curated, state shifted from $IN_REVIEW to $FEDORA_ARCHIVED")
      case FedoraState.DELETED =>
        manager.setProperties(
          "identifier.dans-doi.registered" -> "no",
          "curation.performed" -> "yes",
          "state.label" -> REJECTED.toString,
          "state.description" -> Curation.requestChangesDescription
        )
        manager.saveProperties()
        logAndReturnMessage(s"[$depositId] deposit with datasetId $datasetId has been successfully curated, state shifted from $IN_REVIEW to $REJECTED")
      case fedoraState => logAndReturnMessage(s"[$depositId] deposit with datasetId $datasetId has state $fedoraState, no action required")
    }
  }

  private[managedeposit] def fetchAmdAndExtractState(datasetId: DatasetId, depositId: DepositId): Try[FedoraState] = {
    logger.info(s"[$depositId] retrieving fedora AMD for $datasetId")
    fedora.datasetIdExists(datasetId)
      .flatMap {
        case false =>
          logger.warn(s"[$depositId] datasetId: $depositId was not found in fedora")
          Failure(new IllegalArgumentException(s"datasetId $datasetId was not found in fedora"))
        case true => fedora.getFedoraState(datasetId)
      }
  }

  private def logAndReturnMessage(msg: String): Try[FeedBackMessage] = {
    logger.info(msg)
    Success(msg)
  }
}

object Curation {
  val requestChangesDescription = "The DANS data-manager requests changes on this deposit"
}
