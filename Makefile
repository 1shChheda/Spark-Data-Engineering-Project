.PHONY: help setup build up down run logs clean test

help:
	@echo "Available commands:"
	@echo "  make setup    - Initial project setup"
	@echo "  make build    - Build Docker containers"
	@echo "  make up       - Start containers in background"
	@echo "  make run      - Run the application"
	@echo "  make logs     - View container logs"
	@echo "  make down     - Stop and remove containers"
	@echo "  make clean    - Clean generated files"
	@echo "  make test     - Run tests"

setup:
	@echo "Setting up project..."
	@bash setup.sh

build:
	@echo "Building Docker images..."
	docker-compose -f docker/docker-compose.yml build

up:
	@echo "Starting containers..."
	docker-compose -f docker/docker-compose.yml up -d

run:
	@echo "Running application..."
	docker-compose -f docker/docker-compose.yml up

logs:
	docker-compose -f docker/docker-compose.yml logs -f

down:
	@echo "Stopping containers..."
	docker-compose -f docker/docker-compose.yml down

clean:
	@echo "Cleaning up..."
	rm -rf data/processed/*
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

test:
	docker-compose -f docker/docker-compose.yml run --rm spark-app pytest tests/ -v