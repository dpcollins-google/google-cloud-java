package com.google.cloud.pubsub.v1;

import com.google.api.core.AbstractApiService;

import java.util.List;

abstract class AckProcessor extends AbstractApiService {
  abstract void ack(List<String> ackId);
  abstract void nack(List<String> ackId);
  abstract void extendDeadlines(List<String> ackIds, int deadlineExtensionSeconds);
}
