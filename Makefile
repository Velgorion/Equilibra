ifneq (,$(wildcard ./.env))
    include .env
    export
endif

docker-up:
	docker compose up -d

docker-down:
	docker compose down

psql:
	docker compose exec -it db psql -d $$DB_DSN

migrate-up:
	migrate -database $$DB_DSN -path migrations up

migrate-down:
	migrate -database $$DB_DSN -path migrations down