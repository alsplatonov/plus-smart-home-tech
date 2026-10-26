package ru.yandex.practicum.gateway.security;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.reactive.server.WebTestClientBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpHeaders;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.ServerResponse;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.springframework.web.reactive.function.server.RequestPredicates.path;
import static org.springframework.web.reactive.function.server.RouterFunctions.route;

/**
 * Проверяем только security-правила Gateway, без реальных
 * product-service/inventory-service/order-service: разрешённые запросы
 * отправляются на локальный тестовый бекенд (application.yml),
 * запрещённые должны блокироваться на security с 401/403 раньше,
 * чем Gateway маршрутизирует запрос далее в бекенд
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
@AutoConfigureWebTestClient
class GatewaySecurityConfigTest {

    @Autowired
    private WebTestClient webTestClient;

    @Test
    void catalogGet_isPublic() {
        webTestClient.get().uri("/api/products")
                .exchange()
                .expectStatus().isOk();
    }

    @Test
    void orderCreate_withoutCredentials_isUnauthorized() {
        webTestClient.post().uri("/api/orders")
                .exchange()
                .expectStatus().isUnauthorized();
    }

    @Test
    void orderCreate_withUserCredentials_passesSecurity() {
        webTestClient.post().uri("/api/orders")
                .header(HttpHeaders.AUTHORIZATION, basic("ivan", "ivan"))
                .exchange()
                .expectStatus().isOk();
    }

    @Test
    void productWrite_withUserCredentials_isForbidden() {
        webTestClient.patch().uri("/api/products/1")
                .header(HttpHeaders.AUTHORIZATION, basic("ivan", "ivan"))
                .exchange()
                .expectStatus().isForbidden();
    }

    @Test
    void productWrite_withAdminCredentials_passesSecurity() {
        webTestClient.patch().uri("/api/products/1")
                .header(HttpHeaders.AUTHORIZATION, basic("anna", "anna"))
                .exchange()
                .expectStatus().isOk();
    }

    @Test
    void orderList_withUserCredentials_isForbidden() {
        webTestClient.get().uri("/api/orders")
                .header(HttpHeaders.AUTHORIZATION, basic("ivan", "ivan"))
                .exchange()
                .expectStatus().isForbidden();
    }

    @Test
    void orderList_withAdminCredentials_passesSecurity() {
        webTestClient.get().uri("/api/orders")
                .header(HttpHeaders.AUTHORIZATION, basic("anna", "anna"))
                .exchange()
                .expectStatus().isOk();
    }

    @Test
    void unknownRoute_withAdminCredentials_isForbidden() {
        webTestClient.get().uri("/api/unknown-secret")
                .header(HttpHeaders.AUTHORIZATION, basic("anna", "anna"))
                .exchange()
                .expectStatus().isForbidden();
    }

    @Test
    void corsPreflight_isPublic() {
        webTestClient.options().uri("/api/orders")
                .header(HttpHeaders.ORIGIN, "http://localhost:3000")
                .header("Access-Control-Request-Method", "POST")
                .exchange()
                .expectStatus().isOk();
    }

    private String basic(String username, String password) {
        String value = username + ":" + password;
        return "Basic " + Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    @TestConfiguration
    static class TestBackendConfig {

        @Bean
        RouterFunction<ServerResponse> testBackendRoutes() {
            return route(path("/test-backend"), request -> ServerResponse.ok().build());
        }

        @Bean
        WebTestClientBuilderCustomizer webTestClientBuilderCustomizer() {
            return builder -> builder.baseUrl("http://localhost");
        }
    }
}
