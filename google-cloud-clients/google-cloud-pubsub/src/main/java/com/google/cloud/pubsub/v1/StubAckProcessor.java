package com.google.cloud.pubsub.v1;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.protobuf.Empty;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.ModifyAckDeadlineRequest;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

class StubAckProcessor extends AckProcessor {
  private static final Logger logger =
      Logger.getLogger(StubAckProcessor.class.getName());
  private final SubscriberStub stub;
  private final String subscription;

  private final ApiFutureCallback<Empty> loggingCallback =
      new ApiFutureCallback<Empty>() {
        @Override
        public void onSuccess(Empty empty) {
          // noop
        }

        @Override
        public void onFailure(Throwable t) {
          Level level = isAlive() ? Level.WARNING : Level.FINER;
          logger.log(level, "failed to send operations", t);
        }
      };

  StubAckProcessor(SubscriberStub stub, String subscription) {
    this.stub = stub;
    this.subscription = subscription;
  }

  private boolean isAlive() {
    State state = state(); // Read the state only once.
    return state == State.RUNNING || state == State.STARTING;
  }

  @Override
  protected void doStart() {}

  @Override
  protected void doStop() {}

  @Override
  public void ack(List<String> ackIds) {
    ApiFuture<Empty> future = stub.acknowledgeCallable()
        .futureCall(
            AcknowledgeRequest.newBuilder()
                .setSubscription(subscription)
                .addAllAckIds(ackIds)
                .build());
    ApiFutures.addCallback(future, loggingCallback);
  }

  @Override
  public void nack(List<String> ackIds) {
    extendDeadlines(ackIds, 0);
  }

  @Override
  public void extendDeadlines(List<String> ackIds, int deadlineExtensionSeconds) {
    ApiFuture<Empty> future = stub.modifyAckDeadlineCallable()
        .futureCall(
            ModifyAckDeadlineRequest.newBuilder()
                .setSubscription(subscription)
                .addAllAckIds(ackIds)
                .setAckDeadlineSeconds(deadlineExtensionSeconds)
                .build());
    ApiFutures.addCallback(future, loggingCallback);
  }
}
