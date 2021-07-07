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

import com.yourmediashelf.fedora.client.FedoraClient
import com.yourmediashelf.fedora.client.request.RiSearch
import nl.knaw.dans.easy.managedeposit.FedoraState.FedoraState
import org.apache.commons.lang.BooleanUtils
import resource.{ ManagedResource, managed }

import java.io.InputStream
import scala.io.Source
import scala.util.Try
import scala.xml.{ Elem, XML }

class Fedora(client: FedoraClient) {

  def datasetIdExists(datasetID: DatasetId): Try[Boolean] = {
    executeSparqlQuery(
      s"""prefix dc: <http://purl.org/dc/elements/1.1/identifier>
         |ASK  {?s dc: "$datasetID" }""".stripMargin
    ).flatMap { is => managed(Source.fromInputStream(is)) }
      .map(bs => BooleanUtils.toBoolean(bs.getLines().toList.last))
      .tried
  }

  def getFedoraState(pid: DatasetId): Try[FedoraState] = getAMD(pid).flatMap(extractState)

  private[managedeposit] def extractState(amd: Elem): Try[FedoraState] = Try {
    FedoraState.toState((amd \ "datasetState").text)
      .getOrElse(throw new IllegalArgumentException("No valid state was found"))
  }

  private def getAMD(pid: DatasetId): Try[Elem] = {
    managed { FedoraClient.getDatastreamDissemination(pid, "AMD").execute(client) }
      .flatMap(response => managed { response.getEntityInputStream })
      .map(XML.load)
      .tried
  }

  private def executeSparqlQuery(query: String): ManagedResource[InputStream] = {
    managed(new RiSearch(query).lang("sparql").format("csv").execute(client))
      .flatMap(response => managed(response.getEntityInputStream))
  }
}
