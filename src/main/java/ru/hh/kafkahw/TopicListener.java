package ru.hh.kafkahw;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import ru.hh.kafkahw.internal.Service;

import java.util.HashSet;
import java.util.Set;

@Component
public class TopicListener {
  private final static Logger LOGGER = LoggerFactory.getLogger(TopicListener.class);
  private final Service service;
  private final Set<String> receivedMessages = new HashSet<>();

  @Autowired
  public TopicListener(Service service) {
    this.service = service;
  }

  /*
  Обрабатываем только те сообщения, которые были отправлены с первой попытки.
  Игнорируем исключения возникающие в методе handle()
   */
  @KafkaListener(topics = "topic1", groupId = "group1")
  public void atMostOnce(ConsumerRecord<?, String> consumerRecord, Acknowledgment ack) {
    LOGGER.info("Try handle message, topic {}, payload {}", consumerRecord.topic(), consumerRecord.value());
    String value = consumerRecord.value();
    if (isFirstAttempt(value)) {
      ack.acknowledge();
      try {
        service.handle("topic1", retrieveMessage(value));
      } catch(RuntimeException ignore) {
      }
    }
  }

  /*
  Не важно с какой попытки было отправлено сообщение.
  Исключение в методе handle() обработает дефолтный обработчик и вернет offset на прежнее место
   */
  @KafkaListener(topics = "topic2", groupId = "group2")
  public void atLeastOnce(ConsumerRecord<?, String> consumerRecord, Acknowledgment ack) {
    LOGGER.info("Try handle message, topic {}, payload {}", consumerRecord.topic(), consumerRecord.value());
    String value = consumerRecord.value();
    service.handle("topic2", retrieveMessage(value));
    ack.acknowledge();
  }

  /*
  Чтобы реализовать exactlyOnce было добавлено множество, в котором хранятся обработанные сообщения
   */
  @KafkaListener(topics = "topic3", groupId = "group3")
  public void exactlyOnce(ConsumerRecord<?, String> consumerRecord, Acknowledgment ack) {
    LOGGER.info("Try handle message, topic {}, payload {}", consumerRecord.topic(), consumerRecord.value());
    String value = consumerRecord.value();
    String message = retrieveMessage(value);
    if (isFirstAttempt(value) || !receivedMessages.contains(message)) {
      service.handle("topic3", message);
      ack.acknowledge();
      receivedMessages.add(message);
    }
  }

  /*
  Отделяем сообщения от хвоста с номером попытки
   */
  private String retrieveMessage(String value) {
    int delimiterIndex = value.lastIndexOf('#');
    return value.substring(0, delimiterIndex);
  }

  private boolean isFirstAttempt(String value) {
    int firstNumberIndex = value.lastIndexOf('#') + 1;
    return value.substring(firstNumberIndex).equals("0");
  }
}
