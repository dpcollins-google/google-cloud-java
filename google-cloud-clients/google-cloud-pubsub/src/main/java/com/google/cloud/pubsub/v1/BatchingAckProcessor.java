package com.google.cloud.pubsub.v1;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.gax.batching.*;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.threeten.bp.Duration;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

class BatchingAckProcessor extends AckProcessor {
  private static final Duration ACK_NACK_MAX_DELAY = Duration.ofMillis(100);
  private static final int MAX_PER_REQUEST_CHANGES = 1000;

  final ThresholdBatcher<List<String>> ackBatcher;
  final ThresholdBatcher<List<String>> nackBatcher;

  private final AckProcessor delegate;

  private static FlowController getDisabledFlowController() {
    return new FlowController(
        FlowControlSettings.newBuilder()
            .setLimitExceededBehavior(FlowController.LimitExceededBehavior.Ignore)
            .build());
  }

  @Override
  protected void doStart() {
    delegate.startAsync().awaitRunning();
  }

  @Override
  protected void doStop() {
    ackBatcher.pushCurrentBatch();
    nackBatcher.pushCurrentBatch();
    delegate.stopAsync().awaitTerminated();
  }

  private static class SimpleElementCounter implements ElementCounter<List<String>> {
    @Override
    public long count(List<String> strings) {
      return strings.size();
    }
  }

  private static BatchingFlowController<List<String>> getDisabledBatchingFlowController() {
    return new BatchingFlowController<>(
        getDisabledFlowController(), new SimpleElementCounter(), new SimpleElementCounter());
  }

  private static class ListBatchMerger implements BatchMerger<List<String>> {
    @Override
    public void merge(List<String> batch, List<String> newBatch) {
      batch.addAll(newBatch);
    }
  }

  private class AckBatchReceiver implements ThresholdBatchReceiver<List<String>> {
    @Override
    public void validateBatch(List<String> ackIds) {
      Preconditions.checkArgument(ackIds.size() <= MAX_PER_REQUEST_CHANGES);
    }

    @Override
    public ApiFuture<?> processBatch(List<String> ackIds) {
      delegate.ack(ackIds);
      return ApiFutures.immediateFuture(null);
    }
  }

  private class NackBatchReceiver implements ThresholdBatchReceiver<List<String>> {
    @Override
    public void validateBatch(List<String> ackIds) {
      Preconditions.checkArgument(ackIds.size() <= MAX_PER_REQUEST_CHANGES);
    }

    @Override
    public ApiFuture<?> processBatch(List<String> ackIds) {
      delegate.nack(ackIds);
      return ApiFutures.immediateFuture(null);
    }
  }

  BatchingAckProcessor(ScheduledExecutorService executor, AckProcessor delegate) {
    this.delegate = delegate;
    ackBatcher = ThresholdBatcher.<List<String>>newBuilder()
        .setExecutor(executor)
        .setMaxDelay(ACK_NACK_MAX_DELAY)
        .setThresholds(BatchingThresholds.<List<String>>create(MAX_PER_REQUEST_CHANGES))
        .setReceiver(new AckBatchReceiver())
        .setFlowController(getDisabledBatchingFlowController())
        .setBatchMerger(new ListBatchMerger())
        .build();
    nackBatcher = ThresholdBatcher.<List<String>>newBuilder()
        .setExecutor(executor)
        .setMaxDelay(ACK_NACK_MAX_DELAY)
        .setThresholds(BatchingThresholds.<List<String>>create(MAX_PER_REQUEST_CHANGES))
        .setReceiver(new NackBatchReceiver())
        .setFlowController(getDisabledBatchingFlowController())
        .setBatchMerger(new ListBatchMerger())
        .build();
  }

  @Override
  public void ack(List<String> ackIds) {
    try {
      ackBatcher.add(ackIds);
    } catch (FlowController.FlowControlException e) {
      throw new IllegalStateException("BatchingAckProcessor should not use flow control.", e);
    }
  }

  @Override
  public void nack(List<String> ackIds) {
    try {
      nackBatcher.add(ackIds);
    } catch (FlowController.FlowControlException e) {
      throw new IllegalStateException("BatchingAckProcessor should not use flow control.", e);
    }
  }

  @Override
  public void extendDeadlines(List<String> ackIds, int deadlineExtensionSeconds) {
    for (List<String> idChunk : Lists.partition(ackIds, MAX_PER_REQUEST_CHANGES)) {
      delegate.extendDeadlines(idChunk, deadlineExtensionSeconds);
    }
  }
}
