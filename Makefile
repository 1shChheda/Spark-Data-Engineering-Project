.PHONY: help setup build up down run logs clean test spark-ui status fix-permissions

setup:
	@echo "Setting up project..."
	@bash setup.sh

build:
	@echo "Building Docker images..."
	cd docker && docker-compose build

up:
	@echo "Starting Spark cluster..."
	cd docker && docker-compose up -d
	@echo "Waiting for cluster to initialize..."
	@sleep 10
	@echo "Fixing data directory permissions..."
	@chmod -R 777 data || true
	@echo "✓ Cluster ready!"
	@echo "  Spark Master UI: http://localhost:8080"
	@echo "  Spark Worker UI: http://localhost:8081"

run:
	@echo "Running application..."
	cd docker && docker-compose exec spark-app python -m src.app

logs:
	cd docker && docker-compose logs -f spark-app

spark-ui:
	@echo "Opening Spark Master UI..."
	@cmd /c start http://localhost:8080

status:
	@echo "Cluster Status:"
	cd docker && docker-compose ps

down:
	@echo "Stopping Spark cluster..."
	cd docker && docker-compose down

clean:
	@echo "Cleaning up..."
	rm -rf data/processed/*
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	@chmod -R 777 data || true

test:
	cd docker && docker-compose run --rm spark-app pytest tests/ -v

fix-permissions:
	@echo "Fixing data directory permissions..."
	@chmod -R 777 data
	@cd docker && docker-compose exec spark-master chmod -R 777 /app/data 2>/dev/null || true
	@cd docker && docker-compose exec spark-worker chmod -R 777 /app/data 2>/dev/null || true
	@cd docker && docker-compose exec spark-app chmod -R 777 /app/data 2>/dev/null || true
	@echo "✓ Permissions fixed!"