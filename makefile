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
