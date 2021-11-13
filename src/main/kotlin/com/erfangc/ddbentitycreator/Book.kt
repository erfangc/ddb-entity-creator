package com.erfangc.ddbentitycreator

import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.*
import java.time.Instant
import java.time.LocalDate

@DynamoDbBean
data class Book(
    @get:DynamoDbPartitionKey
    var country: String? = null,
    @get:DynamoDbSortKey
    var isbnNumber: String? = null,
    @get:DynamoDbSecondaryPartitionKey(indexNames = ["idxType", "idxTimeBased"])
    var category: String? = null,
    @get:DynamoDbSecondarySortKey(indexNames = ["idxTimeBased"])
    var publishDate: LocalDate = LocalDate.now(),
    var author: String? = null,
    var publisher: String? = null
)