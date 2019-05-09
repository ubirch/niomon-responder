package com.ubirch.responder

import java.util.UUID

import akka.Done
import akka.kafka.scaladsl.Consumer.DrainingControl
import com.ubirch.kafka.MessageEnvelope
import com.ubirch.niomon.util.KafkaPayload
import com.ubirch.protocol.ProtocolMessage
import net.manub.embeddedkafka.EmbeddedKafka
import org.json4s.JsonAST.{JObject, JString}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.concurrent.ExecutionContext

//noinspection TypeAnnotation
class ResponderMicroserviceTest extends FlatSpec with Matchers with BeforeAndAfterAll with EmbeddedKafka {
  implicit val MESerializer = KafkaPayload[MessageEnvelope].serializer
  implicit val MEDeserializer = KafkaPayload[MessageEnvelope].deserializer

  "ResponderMicroservice" should "produce a valid MessageEnvelope" in {
    publishToKafka("success", MessageEnvelope(new ProtocolMessage()))
    val _ = consumeFirstMessageFrom[MessageEnvelope]("out")
  }

  it should "respond with the response from context, if the message is successful" in {
    publishToKafka("success", MessageEnvelope(new ProtocolMessage(), JObject("configuredResponse" -> JString("hello!"))))
    val res = consumeFirstMessageFrom[MessageEnvelope]("out")
    res.ubirchPacket.getPayload.asText() should equal ("hello!")
  }

  it should "use a default response if configuredResponse is missing from the context" in {
    publishToKafka("success", MessageEnvelope(new ProtocolMessage()))
    val res = consumeFirstMessageFrom[MessageEnvelope]("out")
    res.ubirchPacket.getPayload.toString should equal ("""{"message":"your request has been submitted"}""")
  }

  it should "accept non-MessageEnvelope messages on the error topic" in {
    publishStringMessageToKafka("error", microservice.stringifyException(new RuntimeException("foo"), "key"))
    val res = consumeFirstMessageFrom[MessageEnvelope]("out")
    res.ubirchPacket.getPayload.toString should equal ("""{"error":"RuntimeException: foo","causes":[],"microservice":"responder","requestId":"key"}""")
    res.ubirchPacket.getUUID should equal (UUID.fromString("deaddead-dead-dead-dead-deaddeaddead"))
  }

  var microservice: ResponderMicroservice = _
  var control: DrainingControl[Done] = _

  override def beforeAll(): Unit = {
    EmbeddedKafka.start()
    microservice = new ResponderMicroservice()
    control = microservice.run
  }

  override def afterAll(): Unit = {
    EmbeddedKafka.stop()
    implicit val ec = ExecutionContext.global
    control.drainAndShutdown()
  }
}
