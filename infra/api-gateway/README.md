# API Gateway (Spring Cloud Gateway + Spring Security WebFlux)

## Учебные пользователи (Basic Auth)

Пароли в `gateway.security.users` (в `infra/config-repo/infra/api-gateway.yml` и в
`src/test/resources/application.yml`) хранятся как готовые BCrypt-хеши, а не в открытом
виде. 

Тестовые логин/пароль для локальной проверки и для тестов:

| username | password | roles        |
|----------|----------|--------------|
| ivan     | ivan     | USER         |
| anna     | anna     | USER, ADMIN  |


