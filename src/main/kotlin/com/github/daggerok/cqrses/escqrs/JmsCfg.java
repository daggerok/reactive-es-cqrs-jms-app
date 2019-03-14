package com.github.daggerok.cqrses.escqrs;

import lombok.AllArgsConstructor;
import org.reactivestreams.Publisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.integration.jms.dsl.Jms;
import org.springframework.jms.annotation.EnableJms;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import reactor.core.publisher.Flux;

import javax.jms.ConnectionFactory;
import java.util.Map;
import java.util.Objects;

@EnableJms
@Configuration
@AllArgsConstructor
public class JmsCfg {

  public static final String REACTIVE_QUEUE = "reactive-queue";

  JmsTemplate jmsTemplate;
  ConnectionFactory connectionFactory;

  @Bean
  Publisher<Message<Map<String, String>>> jmsReactivePublisher() {
    return IntegrationFlows.from(Jms.messageDrivenChannelAdapter(connectionFactory)
                                    .destination(REACTIVE_QUEUE))
                           .channel(MessageChannels.queue())
                           //.log(LoggingHandler.Level.DEBUG)
                           .log()
                           .toReactivePublisher();
  }

  @Bean
  Flux<Map<String, String>> sharedStream(Publisher<Message<Map<String, String>>> jmsReactivePublisher) {
    return Flux.from(jmsReactivePublisher)
               .map(JmsCfg::apply)
               .share();
  }

  private static Map<String, String> apply(Message<Map<String, String>> msg) {
    Map<String, String> map = msg.getPayload();
    MessageHeaders headers = msg.getHeaders();
    map.put("timestamp", Objects.requireNonNull(headers.getTimestamp()).toString());
    map.put("id", Objects.requireNonNull(headers.getId()).toString());
    return map;
  }
}
