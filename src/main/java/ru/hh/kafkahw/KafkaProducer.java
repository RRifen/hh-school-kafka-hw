package ru.hh.kafkahw;

import java.util.Random;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class KafkaProducer {
  private final static Logger LOGGER = LoggerFactory.getLogger(KafkaProducer.class);
  private final Random random = new Random();

  private final KafkaTemplate<String, String> kafkaTemplate;

  public KafkaProducer(KafkaTemplate<String, String> kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }

   /*
   Проблема была в том, что исключения могли прервать отправку сообщений в брокер.
   Из-за этого семантики AtLeastOnce и ExactlyOnce не работали.
   В методе send мы повторяем отправку сообщения до тех пор, пока его не отправим.
   */
  public void send(String topic, String payload) {
    boolean isSent = false;
    while(!isSent) {
      isSent = trySend(topic, payload);
    }
  }

  /*
  Данный метод пытается отправить сообщение в кафку.
  В коде могут возникнуть исключения до отправки сообщения и после.
  Исключение, возникающее до отправки, мешало реализовать семантики,
  а исключение после отправки возможно не мешало, так как отправка происходит асинхронно.
  Но я также добавил блокирующий вызов get на CompletableFuture, чтобы быть точно уверенным,
  что сообщение отправилось
   */
  private boolean trySend(String topic, String payload) {
    boolean isSent = false;
    try {
      if (random.nextInt(100) < 10) {
        throw new RuntimeException();
      }
      LOGGER.info("send to kafka, topic {}, payload {}", topic, payload);
      kafkaTemplate.send(topic, payload).get();
      isSent = true;
      if (random.nextInt(100) < 2) {
        throw new RuntimeException();
      }
    } catch(RuntimeException | ExecutionException | InterruptedException ignore) {
    }
    return isSent;
  }
}
