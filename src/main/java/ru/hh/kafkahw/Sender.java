package ru.hh.kafkahw;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.hh.kafkahw.internal.KafkaProducer;

@Component
public class Sender {
  private final KafkaProducer producer;

  @Autowired
  public Sender(KafkaProducer producer) {
    this.producer = producer;
  }

  /*
  В методе send() ошибка может возникнуть либо до отправки, либо после.
  Простое игнорирование исключения не даст реализовать atLeastOnce.
  Если заново отправлять исключение каждый раз, когда возникает ошибка,
  то не получится реализовать atMostOnce
  Поэтому в сообщение был добавлен номер попытки
   */
  public void doSomething(String topic, String message) {
    boolean atLeastOnceSent = false;
    int attemptId = 0;
    while(!atLeastOnceSent) {
      try {
        String payload = message + "#" + attemptId;
        producer.send(topic, payload);
        atLeastOnceSent = true;
      } catch (Exception e) {
        attemptId++;
      }
    }
  }
}
