package com.erfangc.ddbentitycreator

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import java.net.URI

@Configuration
class DbCfg {
    @Bean
    fun dynamoDbClient(): DynamoDbClient {
        return DynamoDbClient.builder().endpointOverride(URI.create("http://localhost:8000")).build()
    }
}