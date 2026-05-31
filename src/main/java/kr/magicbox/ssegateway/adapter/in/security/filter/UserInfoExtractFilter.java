package kr.magicbox.ssegateway.adapter.in.security.filter;

import kr.magicbox.ssegateway.domain.vo.UserId;
import lombok.NonNull;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.ReactiveSecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

@Component
public class UserInfoExtractFilter implements WebFilter {

    @NonNull
    @Override
    public Mono<Void> filter(@NonNull ServerWebExchange exchange, @NonNull WebFilterChain chain) {
        ServerHttpRequest request = exchange.getRequest();

        String userIdHeader = request.getHeaders().getFirst("X-User-Id");
        if (userIdHeader == null || !isValidUserId(userIdHeader)) return chain.filter(exchange);

        UserId userId = UserId.of(Long.parseLong(userIdHeader));
        UsernamePasswordAuthenticationToken authToken =
                new UsernamePasswordAuthenticationToken(userId, null);

        return chain.filter(exchange)
                .contextWrite(ReactiveSecurityContextHolder.withAuthentication(authToken));
    }

    private boolean isValidUserId(String userIdHeader) {
        try {
            return Long.parseLong(userIdHeader) > 0;
        } catch (NumberFormatException e) {
            return false;
        }
    }
}
