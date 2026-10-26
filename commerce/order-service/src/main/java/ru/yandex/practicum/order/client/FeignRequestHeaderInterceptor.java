package ru.yandex.practicum.order.client;

import feign.RequestInterceptor;
import feign.RequestTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import java.util.UUID;

/**
 * Прокидывает технический заголовок X-Request-Id в исходящие Feign-запросы.
 * Если входящий HTTP-запрос уже содержит X-Request-Id, используется он,
 * иначе генерируется новый идентификатор.
 */
@Component
public class FeignRequestHeaderInterceptor implements RequestInterceptor {

    private static final String REQUEST_ID_HEADER = "X-Request-Id";

    @Override
    public void apply(RequestTemplate template) {
        String requestId = resolveIncomingRequestId();
        template.header(REQUEST_ID_HEADER, requestId != null ? requestId : UUID.randomUUID().toString());
    }

    private String resolveIncomingRequestId() {
        if (!(RequestContextHolder.getRequestAttributes() instanceof ServletRequestAttributes attributes)) {
            return null;
        }
        return attributes.getRequest().getHeader(REQUEST_ID_HEADER);
    }
}
