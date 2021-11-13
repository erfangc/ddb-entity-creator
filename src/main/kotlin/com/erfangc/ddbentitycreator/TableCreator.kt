package com.erfangc.ddbentitycreator

import org.slf4j.LoggerFactory
import org.springframework.beans.BeanUtils
import org.springframework.stereotype.Service
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSecondaryPartitionKey
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSecondarySortKey
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSortKey
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.*
import java.lang.reflect.Method

/**
 * Creates DynamoDB table(s) from annotated data classes/POJOs for local testing
 * purposes
 */
@Service
class TableCreator(private val dynamoDbClient: DynamoDbClient) {

    private val log = LoggerFactory.getLogger(TableCreator::class.java)

    fun <T> createTable(
        tableName: String,
        clazz: Class<T>
    ) {

        if (tableExists(tableName)) {
            log.info("The DynamoDB table $tableName already exist, will not attempt to create a new one")
            return
        } else {
            log.info("The DynamoDB table $tableName does not exist - introspecting and creating from annotated entity class")
        }

        val keySchemaElements = scanForKeySchemaElements(clazz)
        val globalSecondaryIndexes = scanForSecondaryIndexes(clazz)
        val attributeDefinitions = scanForAttributeDefinitions(clazz)

        val createTableRequest = CreateTableRequest
            .builder()
            .keySchema(keySchemaElements)
            .globalSecondaryIndexes(globalSecondaryIndexes)
            .tableName(tableName)
            .billingMode(BillingMode.PAY_PER_REQUEST)
            .attributeDefinitions(attributeDefinitions)
            .build()

        doCreateTableRequest(tableName, createTableRequest)
    }

    private fun <T> scanForSecondaryIndexes(
        clazz: Class<T>
    ): ArrayList<GlobalSecondaryIndex> {
        /*
        Collect all the index members as tuples then group by their index name
         */
        val methodsByIndexName = clazz.declaredMethods.filter { method ->
            method.isAnnotationPresent(DynamoDbSecondaryPartitionKey::class.java) ||
                    method.isAnnotationPresent(DynamoDbSecondarySortKey::class.java)
        }.flatMap { method ->
            val pk = method.getAnnotation(DynamoDbSecondaryPartitionKey::class.java)
            val sk = method.getAnnotation(DynamoDbSecondarySortKey::class.java)
            (pk?.indexNames?.map { indexName -> indexName to method } ?: emptyList()) +
                    (sk?.indexNames?.map { indexName -> indexName to method } ?: emptyList())
        }.groupBy { it.first }

        val globalSecondaryIndexes = arrayListOf<GlobalSecondaryIndex>()
        for ((indexName, methods) in methodsByIndexName) {
            val gsiBuilder = GlobalSecondaryIndex.builder()
            gsiBuilder
                .indexName(indexName)
                .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
            val keySchemaElements = arrayListOf<KeySchemaElement>()
            // ensure there is only 1 partitionKey and at most 1 sortKey
            val pks =
                methods.filter { method ->
                    method.second.isAnnotationPresent(DynamoDbSecondaryPartitionKey::class.java)
                }
            if (pks.isEmpty()) {
                error("index $indexName on ${clazz.simpleName} must declare a partition key")
            }
            if (pks.size > 1) {
                error("index $indexName on ${clazz.simpleName} must declare only 1 partition key")
            }
            val pk = pks.first().second
            keySchemaElements.add(
                KeySchemaElement
                    .builder()
                    .keyType(KeyType.HASH)
                    .attributeName(attributeName(pk))
                    .build()
            )
            val sks = methods.filter { method ->
                method.second.isAnnotationPresent(DynamoDbSecondarySortKey::class.java)
            }

            if (sks.size > 1) {
                error("index $indexName on ${clazz.simpleName} must only declare a single sort key")
            }

            if (sks.isNotEmpty()) {
                val sk = sks.first().second
                keySchemaElements.add(
                    KeySchemaElement
                        .builder()
                        .keyType(KeyType.RANGE)
                        .attributeName(attributeName(sk))
                        .build()
                )
            }
            globalSecondaryIndexes.add(gsiBuilder.keySchema(keySchemaElements).build())
        }
        return globalSecondaryIndexes
    }

    private fun <T> scanForKeySchemaElements(clazz: Class<T>): List<KeySchemaElement> {
        val ret = arrayListOf<KeySchemaElement>()
        /*
        Find the hash key
         */
        val pks = clazz
            .declaredMethods
            .filter { method -> method.isAnnotationPresent(DynamoDbPartitionKey::class.java) }
        if (pks.isEmpty()) {
            error("${clazz.simpleName} must declare a partition key")
        }
        if (pks.size > 1) {
            error("${clazz.simpleName} must only declare a single partition key")
        }

        val pkMethod = pks.first()

        ret.add(
            KeySchemaElement
                .builder()
                .keyType(KeyType.HASH)
                .attributeName(attributeName(pkMethod))
                .build()
        )

        /*
        Find the sort key
         */
        val sks = clazz
            .declaredMethods
            .filter { method -> method.isAnnotationPresent(DynamoDbSortKey::class.java) }

        if (sks.size > 1) {
            error("${clazz.simpleName} must only declare a single sort key")
        }

        if (sks.isNotEmpty()) {
            val skMethod = sks.first()
            ret.add(
                KeySchemaElement
                    .builder()
                    .keyType(KeyType.RANGE)
                    .attributeName(attributeName(skMethod))
                    .build()
            )
        }

        return ret
    }

    private fun doCreateTableRequest(
        tableName: String,
        createTableRequest: CreateTableRequest?
    ) {
        log.info("Creating table $tableName createTableRequest=$createTableRequest")
        val response = dynamoDbClient.createTable(createTableRequest)
        val tableDescription = response.tableDescription()
        log.info(
            "CreateTableResponse tableId=${tableDescription.tableId()}, " +
                    "tableName=${tableDescription.tableName()}, " +
                    "tableArn=${tableDescription.tableArn()}, " +
                    "tableStatus=${tableDescription.tableStatus()}"
        )
    }

    private fun tableExists(tableName: String): Boolean {
        val tableExists = try {
            val response = dynamoDbClient.describeTable(DescribeTableRequest.builder().tableName(tableName).build())
            response.table().tableName() == tableName
        } catch (e: ResourceNotFoundException) {
            false
        }
        return tableExists
    }

    private fun <T> scanForAttributeDefinitions(clazz: Class<T>): List<AttributeDefinition> {

        val attributeMethods = clazz.methods.filter { method ->
            val partitionKey = method.isAnnotationPresent(DynamoDbPartitionKey::class.java)
            val sortKey = method.isAnnotationPresent(DynamoDbSortKey::class.java)
            val secondaryPartitionKey = method.isAnnotationPresent(DynamoDbSecondaryPartitionKey::class.java)
            val secondarySortKey = method.isAnnotationPresent(DynamoDbSecondarySortKey::class.java)
            partitionKey || sortKey || secondaryPartitionKey || secondarySortKey
        }

        return attributeMethods.map { method ->
            AttributeDefinition
                .builder()
                .attributeType(attributeType(method))
                .attributeName(attributeName(method))
                .build()
        }
    }

    private fun attributeType(method: Method): ScalarAttributeType {
        val type = method.returnType
        val attributeType = if (
            type == Double::class.java &&
            type == Int::class.java &&
            type == Float::class.java &&
            type == Long::class.java
        ) {
            ScalarAttributeType.N
        } else {
            ScalarAttributeType.S
        }
        return attributeType
    }

    private fun attributeName(method: Method): String {
        val propertyDescriptor = BeanUtils.findPropertyForMethod(method)
            ?: error("Unable to find a property for method $method")
        return propertyDescriptor.name
    }

}