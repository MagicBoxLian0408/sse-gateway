package kr.magicbox.ssegateway.adapter.in.web;

import kr.magicbox.ssegateway.adapter.in.web.dto.response.SseNotificationResponse;
import kr.magicbox.ssegateway.application.port.in.SubscribeSseUseCase;
import kr.magicbox.ssegateway.domain.vo.UserId;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

@RestController
@RequestMapping("/sse")
@RequiredArgsConstructor
public class SseQueryController {

    private final SubscribeSseUseCase subscribeSseUseCase;

    @GetMapping(produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public ResponseEntity<Flux<ServerSentEvent<SseNotificationResponse>>> subscribe(
            @AuthenticationPrincipal UserId userId
    ) {
        HttpHeaders headers = new HttpHeaders();
        headers.set("X-Accel-Buffering", "no");
        headers.set(HttpHeaders.CONNECTION, "keep-alive");
        headers.set(HttpHeaders.CACHE_CONTROL, "no-cache");

        return ResponseEntity.ok()
                .headers(headers)
                .body(subscribeSseUseCase.subscribe(userId));
    }
}
