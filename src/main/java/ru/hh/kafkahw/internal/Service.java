package ru.hh.kafkahw.internal;

import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.stereotype.Component;

@Component
public class Service {

  private final ConcurrentMap<String, ConcurrentMap<String, AtomicInteger>> counters = new ConcurrentHashMap<>();
  private final Random random = new Random();

  /*
  Реализовал ту же логику, что и в продюсере. Ретраим до тех пор, пока не сделаем инкремент,
  и ставим флаг об успешной обработке, до того как вызовется второе исключение.
   */
  public void handle(String topic, String message) {
    boolean isCompute = false;
    while(!isCompute) {
      isCompute = tryHandle(topic, message);
    }
  }

  private boolean tryHandle(String topic, String message) {
    boolean isCompute = false;
    try {
      if (random.nextInt(100) < 10) {
        throw new RuntimeException();
      }
      counters.computeIfAbsent(topic, key -> new ConcurrentHashMap<>())
              .computeIfAbsent(message, key -> new AtomicInteger(0)).incrementAndGet();
      isCompute = true;
      if (random.nextInt(100) < 2) {
        throw new RuntimeException();
      }
    }
    catch(RuntimeException ignore) {
    }
    return isCompute;
  }

  public int count(String topic, String message) {
    return counters.getOrDefault(topic, new ConcurrentHashMap<>()).getOrDefault(message, new AtomicInteger(0)).get();
  }
}
