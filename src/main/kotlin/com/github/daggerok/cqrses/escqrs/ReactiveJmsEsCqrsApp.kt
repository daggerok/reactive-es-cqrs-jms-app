package com.github.daggerok.cqrses.escqrs

import com.github.daggerok.cqrses.escqrs.JmsCfg.REACTIVE_QUEUE
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.io.ClassPathResource
import org.springframework.http.MediaType.APPLICATION_STREAM_JSON
import org.springframework.http.MediaType.TEXT_EVENT_STREAM
import org.springframework.jms.core.JmsTemplate
import org.springframework.web.reactive.function.server.body
import org.springframework.web.reactive.function.server.router
import reactor.core.publisher.Flux
import reactor.core.publisher.toMono
import reactor.core.scheduler.Schedulers

@Configuration
class RestApi(val sharedStream: Flux<Map<String, String>>,
              val jmsTemplate: JmsTemplate) {
  @Bean
  fun routes() = router {
    resources("/", ClassPathResource("/classpath:/static/"))
    //contentType(org.springframework.http.MediaType.APPLICATION_JSON)
    GET("/event-stream") {
      ok().contentType(APPLICATION_STREAM_JSON)
          .contentType(TEXT_EVENT_STREAM)
          .body(sharedStream)
          .subscribeOn(Schedulers.elastic())
    }
    POST("/**") {
      ok().body(it.bodyToMono(Object::class.java)
          .map { jmsTemplate.convertAndSend(REACTIVE_QUEUE, it) }
          .subscribeOn(Schedulers.elastic()))
    }
    path("/**") {
      val uri = it.uri()
      val basePath = "${uri.scheme}://${uri.authority}"
      ok().body(mapOf(
          "_links" to listOf(
              mapOf(
                  "rel" to "_self",
                  "href" to basePath + uri.path,
                  "templated" to false
              )
          )
      ).toMono())
    }
  }
}

@SpringBootApplication
class FbEsCqrsAppApplication

fun main(args: Array<String>) {
  runApplication<FbEsCqrsAppApplication>(*args)
}
