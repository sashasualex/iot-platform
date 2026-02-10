.PHONY: up down build test

up:
	docker compose up --build -d

down:
	docker compose down -v

build:
	gradle clean build -x test

test:
	gradle test jacocoTestReport jacocoTestCoverageVerification
