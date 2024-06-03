/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nageoffer.shortlink.gateway.filter;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.nageoffer.shortlink.gateway.config.Config;
import com.nageoffer.shortlink.gateway.dto.GatewayErrorResult;
import org.springframework.cloud.gateway.filter.GatewayFilter;
import org.springframework.cloud.gateway.filter.factory.AbstractGatewayFilterFactory;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Mono;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

/**
 * SpringCloud Gateway Token 拦截器
 * 这段代码定义了一个Spring Cloud Gateway的自定义过滤器，用于验证请求中的令牌（token）。
 * 如果令牌验证失败，则返回未授权状态。这是一个实现基于Redis存储的令牌验证的过滤器，并通过添加用户信息到请求头来增强请求。
 * 公众号：马丁玩编程，回复：加群，添加马哥微信（备注：link）获取项目资料
 */

@Component
public class TokenValidateGatewayFilterFactory extends AbstractGatewayFilterFactory<Config> {

    private final StringRedisTemplate stringRedisTemplate;

    public TokenValidateGatewayFilterFactory(StringRedisTemplate stringRedisTemplate) {
        super(Config.class);
        this.stringRedisTemplate = stringRedisTemplate;
    }

    /**
     * 在TokenValidateGatewayFilterFactory类中，apply函数的作用是创建并返回一个自定义的GatewayFilter实例。
     * 这个GatewayFilter会在Spring Cloud Gateway处理请求时执行特定的逻辑。在这个例子中，
     * apply方法生成的过滤器用于验证请求中的令牌（token），并根据验证结果决定是否允许请求继续通过网关。
     * @param config
     * @return
     */
    @Override
    public GatewayFilter apply(Config config) {
        /**
         * ServerWebExchange exchange：
         *
         * ServerWebExchange是Spring WebFlux中表示HTTP请求-响应交互的契约。它包含了请求和响应对象，并提供了多种方法来操作这些对象。
         * 主要作用是访问和修改HTTP请求和响应。通过exchange，可以获取请求头、请求路径、HTTP方法等信息，并可以修改响应状态和内容。
         * GatewayFilterChain chain：
         *
         * GatewayFilterChain表示过滤器链，它包含了一系列过滤器，用于在请求到达目标服务之前对请求进行处理，或在响应返回客户端之前对响应进行处理。
         * 主要作用是将当前请求传递给过滤器链中的下一个过滤器。调用chain.filter(exchange)会将处理权交给下一个过滤器。
         *
         * mutate()方法的作用是获取当前对象的一个构建器对象，通过这个构建器对象可以对当前对象的一些属性进行修改，
         * 然后调用build()方法来生成一个新的对象实例。这个新对象将包含所有的修改，并保留未修改的属性。
         */
        return (exchange, chain) -> {
            ServerHttpRequest request = exchange.getRequest();
            String requestPath = request.getPath().toString();
            String requestMethod = request.getMethod().name();
            //判断请求路径和请求方法是否在白名单
            //1. 不在白名单
            if (!isPathInWhiteList(requestPath, requestMethod, config.getWhitePathList())) {
                String username = request.getHeaders().getFirst("username");
                String token = request.getHeaders().getFirst("token");
                Object userInfo;
                //验证令牌, 并且token未过期
                if (StringUtils.hasText(username) && StringUtils.hasText(token) && (userInfo = stringRedisTemplate.opsForHash().get("short-link:login:" + username, token)) != null) {
                    JSONObject userInfoJsonObject = JSON.parseObject(userInfo.toString());
                    //添加用户信息到请求头来增强请求
                    // exchange.getRequest()表示获取请求对象，使用 mutate() 方法创建一个新的 ServerHttpRequest.Builder
                    ServerHttpRequest.Builder builder = exchange.getRequest().mutate().headers(httpHeaders -> {
                        httpHeaders.set("userId", userInfoJsonObject.getString("id"));
                        httpHeaders.set("realName", URLEncoder.encode(userInfoJsonObject.getString("realName"), StandardCharsets.UTF_8));
                    });
                    return chain.filter(exchange.mutate().request(builder.build()).build());
                }
                //如果验证令牌失败则返回未授权状态
                ServerHttpResponse response = exchange.getResponse();
                response.setStatusCode(HttpStatus.UNAUTHORIZED);

                return response.writeWith(Mono.fromSupplier(() -> {
                    DataBufferFactory bufferFactory = response.bufferFactory();
                    GatewayErrorResult resultMessage = GatewayErrorResult.builder()
                            .status(HttpStatus.UNAUTHORIZED.value())
                            .message("Token validation error")
                            .build();
                    return bufferFactory.wrap(JSON.toJSONString(resultMessage).getBytes());
                }));
            }
            //2. 在白名单
            return chain.filter(exchange);
        };
    }

    /**
     * 根据请求路径和请求方法判断是否在白名单
     * @param requestPath 请求路径
     * @param requestMethod 请求方法
     * @param whitePathList 白名单列表
     * @return
     *  true: 在白名单
     *  false：不在白名单
     */
    private boolean isPathInWhiteList(String requestPath, String requestMethod, List<String> whitePathList) {
        return (!CollectionUtils.isEmpty(whitePathList) && whitePathList.stream().anyMatch(requestPath::startsWith))
                || (Objects.equals(requestPath, "/api/short-link/admin/v1/user") && Objects.equals(requestMethod, "POST"));
    }
}
