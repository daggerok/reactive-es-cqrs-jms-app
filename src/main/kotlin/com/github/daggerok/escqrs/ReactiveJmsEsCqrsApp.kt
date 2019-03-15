package com.github.daggerok.escqrs

import org.apache.activemq.broker.BrokerService
import org.apache.activemq.store.PersistenceAdapter
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter
import org.apache.activemq.store.kahadb.MessageDatabase.DEFAULT_DIRECTORY
import org.reactivestreams.Publisher
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary
import org.springframework.context.annotation.Profile
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
import java.io.File
import java.net.URI
import java.nio.file.Paths
import javax.jms.ConnectionFactory

const val reactiveQueue = "reactive-queue"

fun dir(name: String): File {
  val result = Paths.get("target", DEFAULT_DIRECTORY.name, name).toFile()
  result.mkdirs()
  return result
}

@Configuration
@Profile("!embedded")
class Jms {

  @Bean
  fun persistenceAdapter(): PersistenceAdapter {
    val persistenceAdapter = KahaDBPersistenceAdapter()
    persistenceAdapter.directory = dir("db")
    persistenceAdapter.directoryArchive = dir("arch")
    persistenceAdapter.indexDirectory = dir("index")
    return persistenceAdapter
  }

  @Primary
  @Bean(initMethod = "start", destroyMethod = "stop")
  fun brokerService(): BrokerService {
    val broker = BrokerService()
    broker.addConnector("tcp://127.0.0.1:61616")
    //broker.addConnector("vm://127.0.0.1")
    broker.persistenceAdapter = persistenceAdapter()
    broker.isPersistent = true
    return broker
  }
}

@Configuration
class Integration(val connectionFactory: ConnectionFactory) {

  @Bean
  fun jmsReactivePublisher(): Publisher<Message<Map<String, String>>> =
      IntegrationFlows
          .from(Jms.messageDrivenChannelAdapter(connectionFactory)
              .destination(reactiveQueue))
          .channel(MessageChannels.queue())
          .log()
          .toReactivePublisher()

  @Bean
  fun sharedStream() =
      Flux.from(jmsReactivePublisher())
          .map {
            it.payload
                .plus("id" to it.headers.id.toString())
                .plus("timestamp" to it.headers.timestamp.toString())
          }
          .share()
}

@Configuration
class RestApi(val jmsTemplate: JmsTemplate,
              val sharedStream: Flux<Map<String, String>>) {
  @Bean
  fun routes() = router {
    resources("/", ClassPathResource("/classpath:/static/"))

    //contentType(org.springframework.http.MediaType.APPLICATION_JSON)
    GET("/event-stream") {
      ok()//.contentType(org.springframework.http.MediaType.APPLICATION_STREAM_JSON)
          .contentType(TEXT_EVENT_STREAM)
          .body(sharedStream)
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
