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
package nl.knaw.dans.easy

import org.joda.time.format.{ DateTimeFormatter, ISODateTimeFormat }
import resource._

import java.nio.file.{ Files, Path }
import java.util.Optional
import scala.collection.JavaConverters._

package object managedeposit {

  type DepositId = String
  type Deposits = Seq[DepositInformation]
  type DepositorId = String
  type Deposit = Path
  type Datamanager = String
  type Age = Int
  type DatasetId = String

  val XML_NAMESPACE_XSI = "http://www.w3.org/2001/XMLSchema-instance"
  val XML_NAMESPACE_ID_TYPE = "http://easy.dans.knaw.nl/schemas/vocab/identifier-type/"
  val dateTimeFormatter: DateTimeFormatter = ISODateTimeFormat.dateTime()
  val dateFormatter: DateTimeFormatter = ISODateTimeFormat.date()
  val notAvailable = "n/a"

  case class NotReadableException(path: Path, cause: Throwable = null)
    extends Exception(s"""cannot read $path""", cause)

  case class DeleteParameters(filterOnDepositor: Option[DepositorId],
                              age: Int,
                              state: State.State,
                              onlyData: Boolean,
                              doUpdate: Boolean,
                              newState: Option[(State.State, String)] = None,
                              output: Boolean = false,
                             )

  implicit class PathExtensions(val path: Path) extends AnyVal {
    def list[T](f: List[Path] => T): T = {
      managed(Files.list(path)).acquireAndGet(stream => f(stream.iterator().asScala.toList))
    }
  }

  implicit class Optional2Option[T](val opt: Optional[T]) extends AnyVal {
    def toOption: Option[T] = opt.map[Option[T]](Option(_)).orElse(None)
  }
}
