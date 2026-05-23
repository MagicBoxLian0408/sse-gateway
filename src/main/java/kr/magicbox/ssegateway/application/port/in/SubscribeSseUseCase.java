package kr.magicbox.ssegateway.application.port.in;

import kr.magicbox.ssegateway.adapter.in.web.dto.response.SseNotificationResponse;
import kr.magicbox.ssegateway.domain.vo.UserId;
import org.springframework.http.codec.ServerSentEvent;
import reactor.core.publisher.Flux;

public interface SubscribeSseUseCase {

    Flux<ServerSentEvent<SseNotificationResponse>> subscribe(UserId userId);
}
