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
import org.scalatra.{ Ok, ScalatraServlet }

import scala.util.control.NonFatal

class EasyManageDepositServlet(app: EasyManageDepositApp,
                               version: String) extends ScalatraServlet with DebugEnhancedLogging {
  get("/") {
    contentType = "text/plain"
    Ok(s"EASY Manage Deposit running ($version)")
  }




}
