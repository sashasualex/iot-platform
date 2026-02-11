maCOMPOSE = docker compose -f infrastructure/docker-compose.yml

.PHONY: up, down, restart, build, logs, clean

up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

restart:
	$(COMPOSE) down
	$(COMPOSE) up -d

build:
	$(COMPOSE) up -d --build

logs:
	$(COMPOSE) logs -f

clean:
	$(COMPOSE) down -v