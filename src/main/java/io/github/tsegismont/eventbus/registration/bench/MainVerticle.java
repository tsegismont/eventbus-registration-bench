/*
 * Copyright 2021 Red Hat, Inc.
 *
 * Red Hat licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.github.tsegismont.eventbus.registration.bench;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.IntStream;

import static java.util.concurrent.TimeUnit.*;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toList;

public class MainVerticle extends AbstractVerticle {

  private static final int NUM_CONSUMERS = 5000;
  private static final long TARGET_BLACKHOLE = NANOSECONDS.convert(1, MICROSECONDS);


  private final ArrayDeque<MessageConsumer> consumers = new ArrayDeque<>(NUM_CONSUMERS);
  private final long iterationsBlackhole;
  private long timerId;
  private long last = System.nanoTime();

  public MainVerticle() {
    long iterationsForOneMilli = Utils.calibrateBlackhole();
    iterationsBlackhole = Math.round(TARGET_BLACKHOLE * 1.0 * iterationsForOneMilli / Utils.ONE_MILLI_IN_NANO);
  }

  @Override
  public void start() throws Exception {
    vertx.setPeriodic(100, l -> vertx.eventBus().publish("foo", "bar"));
    addConsumers(NUM_CONSUMERS).onSuccess(this::scheduleRemoval).onFailure(this::failure);
  }

  private void scheduleRemoval(Void unused) {
    timerId = vertx.setTimer(1000, l -> {
      removeConsumers(500).onSuccess(this::scheduleCreation).onFailure(this::failure);
    });
  }

  private void scheduleCreation(Void unused) {
    timerId = vertx.setTimer(1000, l -> {
      addConsumers(500).onSuccess(this::scheduleRemoval).onFailure(this::failure);
    });
  }

  private void failure(Throwable t) {
    t.printStackTrace();
    vertx.close();
  }

  private Future<Void> addConsumers(int count) {
    return IntStream.range(0, count)
      .mapToObj(i -> vertx.eventBus().consumer("foo", this::burnCpu))
      .peek(consumers::add)
      .map(consumer-> {
        Promise promise = Promise.promise();
        consumer.completionHandler(promise);
        return promise.future();
      })
      .collect(collectingAndThen(toList(), CompositeFuture::all))
      .mapEmpty();
  }

  private Future<Void> removeConsumers(int count) {
    List<MessageConsumer> list = new ArrayList<>(count);
    try {
      do {
        list.add(consumers.remove());
      } while (list.size() < count);
    } catch (NoSuchElementException e) {
      return Future.failedFuture(e);
    }
    return list.stream()
      .map(consumer -> {
        Promise promise = Promise.promise();
        consumer.unregister(promise);
        return promise.future();
      })
      .collect(collectingAndThen(toList(), CompositeFuture::all))
      .mapEmpty();
  }

  private void burnCpu(Message<String> msg) {
    long start = System.nanoTime();
    Utils.blackholeCpu(iterationsBlackhole);
    if (NANOSECONDS.convert(5, SECONDS) < (start - last)) {
      long stop = System.nanoTime();
      long duration = MICROSECONDS.convert(stop - start, NANOSECONDS);
      System.out.println("duration = " + duration);
      last = stop;
    }
  }

  @Override
  public void stop() throws Exception {
    vertx.cancelTimer(timerId);
  }
}
