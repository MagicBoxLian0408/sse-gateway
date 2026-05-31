package kr.magicbox.ssegateway.adapter.in.security.filter;

import kr.magicbox.ssegateway.adapter.in.security.properties.TrustedIpProperties;
import kr.magicbox.ssegateway.domain.vo.UserId;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.ReactiveSecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;

@Component
@RequiredArgsConstructor
public class UserInfoExtractFilter implements WebFilter {

    private final TrustedIpProperties trustedIpProperties;

    @NonNull
    @Override
    public Mono<Void> filter(@NonNull ServerWebExchange exchange, @NonNull WebFilterChain chain) {
        ServerHttpRequest request = exchange.getRequest();
        InetSocketAddress remoteAddress = request.getRemoteAddress();

        if (remoteAddress == null) return chain.filter(exchange);

        String clientIp = remoteAddress.getAddress().getHostAddress();
        if (!isTrustedIp(clientIp, trustedIpProperties.getIps())) return chain.filter(exchange);

        String userIdHeader = request.getHeaders().getFirst("X-User-Id");
        if (userIdHeader == null || !isValidUserId(userIdHeader)) return chain.filter(exchange);

        UserId userId = UserId.of(Long.parseLong(userIdHeader));
        UsernamePasswordAuthenticationToken authToken =
                new UsernamePasswordAuthenticationToken(userId, null);

        return chain.filter(exchange)
                .contextWrite(ReactiveSecurityContextHolder.withAuthentication(authToken));
    }

    private boolean isTrustedIp(String clientIp, List<String> trustedIps) {
        return trustedIps.stream().anyMatch(trusted -> matchesCidr(trusted, clientIp));
    }

    private boolean matchesCidr(String cidr, String clientIp) {
        try {
            if (!cidr.contains("/")) return cidr.equals(clientIp);
            String[] parts = cidr.split("/");
            int prefixLen = Integer.parseInt(parts[1]);
            byte[] cidrBytes = InetAddress.getByName(parts[0]).getAddress();
            byte[] clientBytes = InetAddress.getByName(clientIp).getAddress();
            if (cidrBytes.length != clientBytes.length) return false;
            int mask = prefixLen == 0 ? 0 : (0xFFFFFFFF << (32 - prefixLen));
            int cidrInt = toInt(cidrBytes) & mask;
            int clientInt = toInt(clientBytes) & mask;
            return cidrInt == clientInt;
        } catch (Exception e) {
            return false;
        }
    }

    private int toInt(byte[] bytes) {
        return ((bytes[0] & 0xFF) << 24) | ((bytes[1] & 0xFF) << 16) | ((bytes[2] & 0xFF) << 8) | (bytes[3] & 0xFF);
    }

    private boolean isValidUserId(String userIdHeader) {
        try {
            return Long.parseLong(userIdHeader) > 0;
        } catch (NumberFormatException e) {
            return false;
        }
    }
}
