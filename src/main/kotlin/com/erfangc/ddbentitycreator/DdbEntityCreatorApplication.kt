package com.erfangc.ddbentitycreator

import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean

@SpringBootApplication
class DdbEntityCreatorApplication {
	@Bean
	fun cmdRunner(tableCreator: TableCreator): CommandLineRunner {
		return CommandLineRunner {
			tableCreator.createTable("books", Book::class.java)
		}
	}
}

fun main(args: Array<String>) {
	runApplication<DdbEntityCreatorApplication>(*args)
}
