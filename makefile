# Make the deployment of updated docker containers quick and easy!
# If you're not using AWS then replace aws commands with GCP commands for exmaple - or other cloud providers.
# Make pushing to github repo quick and easy

include .env

# Docker and AWS section
# AWS ECR Docker deployment configuration
.PHONY: docker-all docker-login docker-build docker-tag docker-push docker-verify

docker-all: docker-login docker-build docker-tag docker-push docker-verify

docker-login:
	aws ecr get-login-password --region $(REGION) | docker login --username AWS --password-stdin $(ACCOUNT_ID).dkr.ecr.$(REGION).amazonaws.com

docker-build:
	docker buildx build --platform=linux/amd64 -t $(REPO_NAME) .

docker-tag:
	docker tag $(REPO_NAME):latest $(ACCOUNT_ID).dkr.ecr.$(REGION).amazonaws.com/$(REPO_NAME):latest

docker-push:
	docker push $(ACCOUNT_ID).dkr.ecr.$(REGION).amazonaws.com/$(REPO_NAME):latest

docker-verify:
	aws ecr describe-images --repository-name $(REPO_NAME) --region $(REGION)

# PostgreSQL Docker section
.PHONY: postgres-start postgres-stop postgres-restart postgres-logs postgres-clean postgres-status

# Default PostgreSQL configuration (can be overridden in .env)
POSTGRES_DB ?= street_works_data
POSTGRES_USER ?= postgres
POSTGRES_PASSWORD ?= postgres123
POSTGRES_PORT ?= 5432
POSTGRES_CONTAINER_NAME ?= postgres-street-works

postgres-start:
	@echo "Starting PostgreSQL container..."
	docker run -d \
		--name $(POSTGRES_CONTAINER_NAME) \
		-e POSTGRES_DB=$(POSTGRES_DB) \
		-e POSTGRES_USER=$(POSTGRES_USER) \
		-e POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) \
		-p $(POSTGRES_PORT):5432 \
		-v postgres_data:/var/lib/postgresql/data \
		postgres:15-alpine
	@echo "PostgreSQL container started on port $(POSTGRES_PORT)"
	@echo "Database: $(POSTGRES_DB)"
	@echo "Username: $(POSTGRES_USER)"
	@echo "Password: $(POSTGRES_PASSWORD)"

postgres-stop:
	@echo "Stopping PostgreSQL container..."
	docker stop $(POSTGRES_CONTAINER_NAME) || true
	docker rm $(POSTGRES_CONTAINER_NAME) || true

postgres-restart: postgres-stop postgres-start

postgres-logs:
	docker logs -f $(POSTGRES_CONTAINER_NAME)

postgres-status:
	@echo "PostgreSQL container status:"
	@docker ps -a --filter name=$(POSTGRES_CONTAINER_NAME) --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

postgres-clean: postgres-stop
	@echo "Cleaning up PostgreSQL data volume..."
	docker volume rm postgres_data || true

postgres-connect:
	@echo "Connecting to PostgreSQL..."
	docker exec -it $(POSTGRES_CONTAINER_NAME) psql -U $(POSTGRES_USER) -d $(POSTGRES_DB)

# Git section
.PHONY: update git-add git-commit git-push

DATE := $(shell date +%Y-%m-%d)

update: git-add git-commit git-push

git-add:
	git add .

git-commit:
	@read -p "Please enter an additional commit message: " msg; \
	git commit -m "updates $(DATE) - $$msg"

git-push:
	git push
