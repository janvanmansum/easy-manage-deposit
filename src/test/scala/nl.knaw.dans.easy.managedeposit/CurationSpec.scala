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

import better.files.File
import nl.knaw.dans.easy.managedeposit.Command.FeedBackMessage
import nl.knaw.dans.easy.managedeposit.FedoraState.{ DELETED, FedoraState, MAINTENANCE, PUBLISHED }
import nl.knaw.dans.easy.managedeposit.State._
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterEach

import java.net.URI
import scala.util.{ Failure, Success, Try }

class CurationSpec extends TestSupportFixture
  with Curation
  with MockFactory
  with BeforeAndAfterEach {

  override val fedora: Fedora = mock[Fedora]
  override val landingPageBaseUrl: URI = new URI("http://deasy.dans.knaw.nl/ui/datasets/id/")
  private val datasetId = "easy-dataset:12345"

  override def beforeEach(): Unit = {
    super.beforeEach()
    depositDir.clear()
    File(getClass.getResource("/inputForEasyManageDeposit/").toURI).copyTo(depositDir)
  }

  "fetchAmdAndExtractState" should "return Published if fedora returns published" in {
    (fedora.datasetIdExists(_: DatasetId)) expects "easy-dataset:1" returning Success(true)
    (fedora.getFedoraState(_: DatasetId)) expects "easy-dataset:1" returning Success(PUBLISHED)
    fetchAmdAndExtractState("easy-dataset:1", depositOnePath.toString) shouldBe Success(PUBLISHED)
  }

  it should "fail if a unrecognized state is retrieved from fedora" in {
    (fedora.datasetIdExists(_: DatasetId)) expects "easy-dataset:1" returning Success(true)
    (fedora.getFedoraState(_: DatasetId)) expects "easy-dataset:1" returning Failure(new IllegalArgumentException("No valid state was found"))
    fetchAmdAndExtractState("easy-dataset:1", depositOnePath.toString) should matchPattern {
      case Failure(iae: IllegalArgumentException) if iae.getMessage == "No valid state was found" =>
    }
  }

  it should "fail if no state is retrieved from fedora" in {
    (fedora.datasetIdExists(_: DatasetId)) expects "easy-dataset:1" returning Success(true)
    (fedora.getFedoraState(_: DatasetId)) expects "easy-dataset:1" returning Failure(new IllegalArgumentException("No valid state was found"))
    fetchAmdAndExtractState("easy-dataset:1", depositOnePath.toString) should matchPattern {
      case Failure(iae: IllegalArgumentException) if iae.getMessage == "No valid state was found" =>
    }
  }

  "curate" should "should update the state.label=FEDORA_ARCHIVED, state.description=<landingPage> and curation.performed properties if the current state.label is IN_REVIEW" in {
    val manager = setupTestAndValidateCurationResult(PUBLISHED, Success(s"[aba410b6-1a55-40b2-9ebe-6122aad00285] deposit with datasetId $datasetId has been successfully curated, state shifted from $IN_REVIEW to $FEDORA_ARCHIVED"))
    manager.getStateLabel shouldBe FEDORA_ARCHIVED
    manager.getStateDescription.value shouldBe s"$landingPageBaseUrl$datasetId"
    manager.isCurationPerformed shouldBe true
  }

  it should "fail if there is not easyDataSet-id provided" in {
    val manager = new DepositManager(depositOnePath)
    manager.setProperty("identifier.fedora", null)
    manager.saveProperties()
    curate(manager) should matchPattern {
      case Failure(ise: IllegalStateException) if ise.getMessage == s"[${ manager.getDepositId }] no datasetId found during curation" =>
    }
  }

  it should "not do anything if the state.label is not IN_REVIEW" in {
    val manager = new DepositManager(depositOnePath)
    manager.isCurationPerformed shouldBe false
    curate(manager) shouldBe Success(s"[aba410b6-1a55-40b2-9ebe-6122aad00285] deposit with datasetId $datasetId has state $SUBMITTED, no action required")
    manager.getStateLabel shouldBe SUBMITTED
    manager.isCurationPerformed shouldBe false
  }

  it should "should update the state.label=REJECTED, state.description and curation.performed properties if the current state.label is IN_REVIEW and fedora.state=DELETED" in {
    val manager = setupTestAndValidateCurationResult(DELETED, Success(s"[aba410b6-1a55-40b2-9ebe-6122aad00285] deposit with datasetId $datasetId has been successfully curated, state shifted from $IN_REVIEW to $REJECTED"))
    manager.getStateLabel shouldBe REJECTED
    manager.getStateDescription.value shouldBe Curation.requestChangesDescription
    manager.isCurationPerformed shouldBe true
  }

  it should "not do anything if a fedoraState is returned that is not published or deleted" in {
    val manager = setupTestAndValidateCurationResult(MAINTENANCE, Success(s"[aba410b6-1a55-40b2-9ebe-6122aad00285] deposit with datasetId $datasetId has state $MAINTENANCE, no action required"))
    manager.getStateLabel shouldBe IN_REVIEW
    manager.getStateDescription.value shouldBe "Deposit is valid and ready for post-submission processing"
    manager.isCurationPerformed shouldBe false
  }

  it should "not do anything if no fedoraState is returned" in {
    val manager = setupTestAndValidateCurationResult(null, Success(s"[aba410b6-1a55-40b2-9ebe-6122aad00285] deposit with datasetId $datasetId has state null, no action required"))
    manager.getStateLabel shouldBe IN_REVIEW
    manager.getStateDescription.value shouldBe "Deposit is valid and ready for post-submission processing"
    manager.isCurationPerformed shouldBe false
  }

  it should "not do anything if there is no state" in {
    val manager = new DepositManager(depositOnePath)
    manager.setProperty("state.label", null)
    manager.getStateLabel shouldBe UNKNOWN
    curate(manager) shouldBe Success(s"[${ manager.getDepositId.value }] deposit with datasetId $datasetId has state $UNKNOWN, no action required")
    manager.getStateDescription.value shouldBe "Deposit is valid and ready for post-submission processing"
    manager.isCurationPerformed shouldBe false
    manager.getStateLabel shouldBe UNKNOWN
  }

  it should "fail if the dataset is not known at fedora" in {
    val manager = new DepositManager(depositOnePath)
    manager.setState(IN_REVIEW)
    manager.getStateLabel shouldBe IN_REVIEW
    (fedora.datasetIdExists(_: DatasetId)) expects datasetId returning Success(false)
    curate(manager) should matchPattern {
      case Failure(iae: IllegalArgumentException) if iae.getMessage == s"datasetId $datasetId was not found in fedora" =>
    }
    manager.getStateDescription.value shouldBe "Deposit is valid and ready for post-submission processing"
    manager.isCurationPerformed shouldBe false
    manager.getStateLabel shouldBe IN_REVIEW
  }

  it should "not do anything if the http-request to fedora fails" in {
    val manager = new DepositManager(depositOnePath)
    manager.setState(IN_REVIEW)
    manager.getStateLabel shouldBe IN_REVIEW
    (fedora.datasetIdExists(_: DatasetId)) expects datasetId returning Failure(new RuntimeException("Connection Refused"))
    curate(manager) should matchPattern {
      case Failure(rte: RuntimeException) if rte.getMessage == "Connection Refused" =>
    }
    manager.getStateDescription.value shouldBe "Deposit is valid and ready for post-submission processing"
    manager.isCurationPerformed shouldBe false
  }

  private def setupTestAndValidateCurationResult(fedoraState: FedoraState, result: Try[FeedBackMessage]): DepositManager = {
    val manager = new DepositManager(depositOnePath)
    manager.isCurationPerformed shouldBe false
    manager.setState(IN_REVIEW)
    manager.getStateLabel shouldBe IN_REVIEW
    (fedora.datasetIdExists(_: DatasetId)) expects datasetId returning Success(true)
    (fedora.getFedoraState(_: DatasetId)) expects datasetId returning Try(fedoraState)
    curate(manager) shouldBe result
    manager
  }
}
