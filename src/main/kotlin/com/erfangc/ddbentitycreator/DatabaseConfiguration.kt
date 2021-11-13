package com.erfangc.ddbentitycreator

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import java.net.URI

@Configuration
class DatabaseConfiguration {
    @Bean
    fun dynamoDbClient(): DynamoDbClient {
        return DynamoDbClient
            .builder()
            .build()
    }
}