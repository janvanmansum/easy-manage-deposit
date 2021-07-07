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
import nl.knaw.dans.easy.managedeposit.State._
import org.apache.commons.configuration.PropertiesConfiguration
import org.scalatest.BeforeAndAfterEach
import org.scalatest.Inside.inside

import java.net.URI
import java.nio.file.Path
import scala.util.Success

class EasyManageDepositSpec extends TestSupportFixture with BeforeAndAfterEach {

  private val app = new EasyManageDepositApp(DummyConfiguration)

  override def beforeEach(): Unit = {
    super.beforeEach()
    depositDir.clear()
    File(getClass.getResource("/inputForEasyManageDeposit/").toURI).copyTo(depositDir)
    (depositDir / "deposit-no-properties").delete() // delete deposit that would let the 'deleteDepositsFromDepositsDir' fail
  }

  "deleteDepositsFromDepositsDir" should "return a list of two DepositInformation items" in {
    val deleteParameters = DeleteParameters(Some("user001"), age = 1, state = SUBMITTED, onlyData = false, doUpdate = true)
    val result = app.deleteDepositsFromDepositsDir(depositDir.path, deleteParameters, "SWORD2")
    inside(result) {
      case Success(n) =>
        n should have size 2
    }
  }

  it should "make the size of depositDir 2 items smaller" in {
    val sizeOriginal = depositDir.list.size
    val deleteParameters = DeleteParameters(Some("user001"), age = 1, state = SUBMITTED, onlyData = false, doUpdate = true)
    app.deleteDepositsFromDepositsDir(depositDir.path, deleteParameters, "SWORD2")
    depositDir.list.size shouldBe sizeOriginal - 2
  }

  it should "return an empty list when no deposits to delete" in {
    val deleteParameters = DeleteParameters(Some("user001"), age = 1, state = UNKNOWN, onlyData = false, doUpdate = true)
    val result = app.deleteDepositsFromDepositsDir(depositDir.path, deleteParameters, "SWORD2")
    inside(result) {
      case Success(n) =>
        n should have size 0
    }
  }
}
