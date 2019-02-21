package com.google.cloud.pubsub.v1;

import com.google.api.core.ApiClock;
import com.google.api.core.CurrentMillisClock;
import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.batching.FlowController;
import com.google.api.gax.core.Distribution;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.ReceivedMessage;
import org.junit.Test;
import org.threeten.bp.Duration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class MessageDispatcherLoadTest {
  private static final int MESSAGE_SIZE = 1000;
  private static final int BATCH_SIZE = 10000000 / MESSAGE_SIZE;
  private static final ReceivedMessage.Builder MESSAGE_TEMPLATE =
      ReceivedMessage.newBuilder()
          .setMessage(PubsubMessage.newBuilder().setData(createMessage(MESSAGE_SIZE)).build());
  private static final int RUNTIME_MILLIS = 120 * 1000;

  private static ByteString createMessage(int msgSize) {
    byte[] payloadArray = new byte[msgSize];
    Arrays.fill(payloadArray, (byte) 'A');
    return ByteString.copyFrom(payloadArray);
  }

  private static List<ReceivedMessage> createBatch(int iteration) {
    List<ReceivedMessage> toReturn = new ArrayList<>(BATCH_SIZE);
    for (int i = 0; i < BATCH_SIZE; i++) {
      toReturn.add(MESSAGE_TEMPLATE.clone().setAckId("ack-" + iteration + "-" + i).build());
    }
    return toReturn;
  }

  @Test
  public void testThroughput() throws Exception {
    MessageReceiver receiver = new MessageReceiver() {
      @Override
      public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
        consumer.ack();
      }
    };

    final AtomicInteger ackCount = new AtomicInteger(0);
    MessageDispatcher.AckProcessor processor = new MessageDispatcher.AckProcessor() {
      @Override
      public void sendAckOperations(
          List<String> acksToSend, List<MessageDispatcher.PendingModifyAckDeadline> ackDeadlineExtensions) {
        ackCount.getAndAdd(acksToSend.size());
      }
    };

    ApiClock clock = CurrentMillisClock.getDefaultClock();

    MessageDispatcher dispatcher = new MessageDispatcher(
        receiver,
        processor,
        Duration.ofSeconds(5),
        Duration.ofMinutes(60),
        new Distribution(Subscriber.MAX_ACK_DEADLINE_SECONDS + 1),
        new FlowController(
            FlowControlSettings.newBuilder()
                .setMaxOutstandingRequestBytes(1000L * 1000L * 1000L)  // max 1GB outstanding.
                .build()),
        Executors.newFixedThreadPool(5 *  Runtime.getRuntime().availableProcessors()),
        Executors.newScheduledThreadPool(6),
        clock);
    dispatcher.start();

    int iteration = 0;
    long startTime = clock.millisTime();
    while ((clock.millisTime() - startTime) < RUNTIME_MILLIS) {
      dispatcher.processReceivedMessages(createBatch(iteration));
      iteration++;
      if (iteration % 100 == 0) {
        System.out.println("Iteration: " + iteration + ", Time: " + ((clock.millisTime() - startTime) / 1000));
      }
    }

    System.out.println("Throughput: " + (ackCount.get() / (RUNTIME_MILLIS * 1.0 / 1000)) + " messages per second.");
  }
}