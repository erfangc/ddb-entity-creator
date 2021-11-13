package com.erfangc.ddbentitycreator

import org.springframework.web.bind.annotation.*
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient
import software.amazon.awssdk.enhanced.dynamodb.Key
import software.amazon.awssdk.enhanced.dynamodb.TableSchema
import software.amazon.awssdk.enhanced.dynamodb.model.PutItemEnhancedRequest
import software.amazon.awssdk.services.dynamodb.DynamoDbClient

@RestController
@RequestMapping("books")
class BooksController(dynamoDbClient: DynamoDbClient) {
    private val dynamoDbEnhancedClient = DynamoDbEnhancedClient
        .builder()
        .dynamoDbClient(dynamoDbClient)
        .build()
    private val table = dynamoDbEnhancedClient.table("books", TableSchema.fromBean(Book::class.java))

    @PostMapping
    fun createBook(@RequestBody book: Book) {
        table.putItem(
            PutItemEnhancedRequest
                .builder(Book::class.java)
                .item(book)
                .build()
        )
    }

    @GetMapping("{country}/{isbnNumber}")
    fun getBook(@PathVariable isbnNumber: String, @PathVariable country: String): Book {
        return table.getItem(
            Key
                .builder()
                .partitionValue(country)
                .sortValue(isbnNumber)
                .build()
        ) ?: error("Book not found")
    }
}