package ru.hh.kafkahw;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import ru.hh.kafkahw.internal.Service;

@Component
public class TopicListener {
  private final static Logger LOGGER = LoggerFactory.getLogger(TopicListener.class);
  private final Service service;

  @Autowired
  public TopicListener(Service service) {
    this.service = service;
  }

  /*
  Для семантики atMostOnce достаточно просто проигнорировать исключение.
  Сообщение может не успеть обработаться, но это не страшно.
  Исключение необходимо ловить в блоке try, потому что обработчик ошибок по умолчанию
  заново прочитает сообщение из брокера, даже если успеть закоммитить offset (ack.acknowledge)
   */
  @KafkaListener(topics = "topic1", groupId = "group1")
  public void atMostOnce(ConsumerRecord<?, String> consumerRecord, Acknowledgment ack) {
    LOGGER.info("Try handle message, topic {}, payload {}", consumerRecord.topic(), consumerRecord.value());
    ack.acknowledge();
    try {
      service.handle("topic1", consumerRecord.value());
    } catch(RuntimeException ignored) {
    }
  }

  /*
  Для семантики atLeastOnce по сути надо было только исправить Producer.
  Дефолтный обработчик ошибок сам вернет offset если вылезло исключение.
  При этом одно и то же сообщение может обработаться более одного раза,
  так как исключение может вылезти уже после обработки сообщения
   */
  @KafkaListener(topics = "topic2", groupId = "group2")
  public void atLeastOnce(ConsumerRecord<?, String> consumerRecord, Acknowledgment ack) {
    LOGGER.info("Try handle message, topic {}, payload {}", consumerRecord.topic(), consumerRecord.value());
    service.handle("topic2", consumerRecord.value());
    ack.acknowledge();
  }

  /*
  Здесь все сложно. Так как ошибка может возникнуть либо до, либо после обработки,
  то без изменения класса Service не обойтись, можно реализовать тот же механизм ретраев, как в продюсере.
  Либо можно реализовать семантику atLeastOnce, тогда вероятность того,
  что сообщение обработается более одного раза будет 2% - random.nextInt(100) < 2.
   */
  @KafkaListener(topics = "topic3", groupId = "group3")
  public void exactlyOnce(ConsumerRecord<?, String> consumerRecord, Acknowledgment ack) {
    LOGGER.info("Try handle message, topic {}, payload {}", consumerRecord.topic(), consumerRecord.value());
    service.handle("topic3", consumerRecord.value());
    ack.acknowledge();
  }
}
