package com.github.daggerok.escqrs

import org.reactivestreams.Publisher
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.io.ClassPathResource
import org.springframework.http.MediaType.TEXT_EVENT_STREAM
import org.springframework.integration.dsl.IntegrationFlows
import org.springframework.integration.dsl.MessageChannels
import org.springframework.integration.jms.dsl.Jms
import org.springframework.jms.core.JmsTemplate
import org.springframework.messaging.Message
import org.springframework.web.reactive.function.server.ServerRequest
import org.springframework.web.reactive.function.server.body
import org.springframework.web.reactive.function.server.router
import reactor.core.publisher.Flux
import reactor.core.publisher.toMono
import reactor.core.scheduler.Schedulers
import java.net.URI
import javax.jms.ConnectionFactory

const val reactiveQueue = "reactive-queue"

@Configuration
class Cfg(val jmsTemplate: JmsTemplate,
          val connectionFactory: ConnectionFactory) {
  @Bean
  fun jmsReactivePublisher(): Publisher<Message<MutableMap<String, String>>> =
      IntegrationFlows
          .from(Jms.messageDrivenChannelAdapter(connectionFactory)
              .destination(reactiveQueue))
          .channel(MessageChannels.queue())
          .log()
          .toReactivePublisher()

  @Bean
  fun sharedStream() =
      Flux
          .from(jmsReactivePublisher())
          .map {
            val headers = it.headers
            val payload = it.payload
            payload["timestamp"] = headers.timestamp.toString()
            payload["id"] = headers.id.toString()
            payload.toMap()
          }
          .share()

  @Bean
  fun routes() = router {
    resources("/", ClassPathResource("/classpath:/static/"))

    //contentType(org.springframework.http.MediaType.APPLICATION_JSON)
    GET("/event-stream") {
      ok()//.contentType(org.springframework.http.MediaType.APPLICATION_STREAM_JSON)
          .contentType(TEXT_EVENT_STREAM)
          .body(sharedStream())
          .subscribeOn(Schedulers.elastic())
    }

    fun ServerRequest.baseUrl() = "${this.uri().scheme}://${this.uri().authority}"

    POST("/**") {
      created(URI.create("${it.baseUrl()}/event-stream"))
          .body(it.bodyToMono(Object::class.java)
              .map { jmsTemplate.convertAndSend(reactiveQueue, it) }
              .then("Message sent.".toMono())
              .subscribeOn(Schedulers.elastic()))
    }

    path("/**") {
      ok().body(mapOf(
          "_links" to listOf(
              mapOf(
                  "rel" to "_self",
                  "href" to it.baseUrl() + it.path(),
                  "templated" to false,
                  "method" to "*"
              ),
              mapOf(
                  "rel" to "send",
                  "href" to it.baseUrl(),
                  "templated" to false,
                  "method" to "POST"
              ),
              mapOf(
                  "rel" to "subscribe",
                  "href" to "${it.baseUrl()}/event-stream",
                  "templated" to false,
                  "method" to "GET"
              )
          )
      ).toMono())
    }
  }
}

@SpringBootApplication
class ReactiveJmsEsCqrsApp

fun main(args: Array<String>) {
  runApplication<ReactiveJmsEsCqrsApp>(*args)
}