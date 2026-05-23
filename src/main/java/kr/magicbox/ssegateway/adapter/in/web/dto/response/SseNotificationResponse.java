package kr.magicbox.ssegateway.adapter.in.web.dto.response;

import lombok.Builder;

@Builder
public record SseNotificationResponse(
        String purchaseToken
) {}
