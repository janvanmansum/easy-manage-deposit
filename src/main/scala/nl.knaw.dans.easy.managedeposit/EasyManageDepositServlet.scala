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
import org.scalatra._

import scala.util.control.NonFatal
import scala.util.{ Failure, Success, Try }

class EasyManageDepositServlet(app: EasyManageDepositApp,
                               version: String) extends ScalatraServlet with DebugEnhancedLogging {
  get("/") {
    contentType = "text/plain"
    Ok(s"EASY Manage Deposit running ($version)")
  }

  put("/deposits/:uuid") {
    val result = for {
      _ <- checkContentType("text/plain")
      props <- app.readDepositProperties(request.body)
      uuid = params("uuid")
      _ = debug(s"Found parameter uuid = $uuid")
      // TODO: check UUID is valid UUID
      _ <- app.saveDepositProperties(uuid, props, request.body)
    } yield ()

    result match {
      case Success(_) => Ok()
      case Failure(e: IllegalArgumentException) if e.getMessage.startsWith("Media type") => UnsupportedMediaType(e.getMessage)
      case Failure(e: IllegalArgumentException) => BadRequest(e.getMessage)
      case Failure(NonFatal(e)) => InternalServerError(e.getMessage)
    }
  }

  get("/deposits/:uuid") {
    contentType = "text/plain"
    val uuid = params("uuid")
    app.getDepositProperties(uuid) match {
      case Success(Some(propsText)) => Ok(propsText)
      case Success(None) => NotFound(s"No deposit.properties found for uuid = $uuid")
      case Failure(NonFatal(e)) => InternalServerError(e.getMessage)
    }
  }

  private def checkContentType(expected: String): Try[Unit] = {
    if (request.contentType.contains(expected)) Success(())
    else Failure(new IllegalArgumentException(s"Media type must be '$expected', found '${ request.contentType.getOrElse("nothing") }'"))
  }
}
