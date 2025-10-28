# Data Engineering Project
A containerized **Apache Spark** setup for running and testing data engineering workflows locally
This setup runs:

* **1 Spark Master ( Cluster Manager )**
* **1 Spark Worker**
* **1 Application Container ( Driver )** (for your Python code)

## Prerequisites

Make sure you have the following installed:

- Docker Desktop (with WSL2 backend on Windows)
- Make (optional, but recommended)

## Quick Start

### 1. Initial Setup
```bash
make setup
```

### 2. Build Docker Images

```bash
make build
```

### 3. Start the Spark Cluster
```bash
make up
```

* Spark Master UI → [http://localhost:8080](http://localhost:8080)
* Spark Worker UI → [http://localhost:8081](http://localhost:8081)

### 4. Run Application
```bash
make run
```

This runs `python -m src.app` inside the `spark-app` container.

If you encounter file permission errors (e.g., while writing output),
you can reset permissions using:

```bash
make fix-permissions
```

This ensures all containers have full read/write access to the shared `data/` directory.

## Available Commands

* `make setup` - **One-time setup** (install, permissions, env config)
* `make build` - Build Docker containers
* `make up` - Start the Spark cluster (Master + Worker + App)
* `make run` - Run the Spark application
* `make fix-permissions` - Fix shared data directory permissions
* `make logs` - View application logs
* `make spark-ui` - Open Spark Master UI in browser
* `make status` - Check container status
* `make down` - Stop containers
* `make clean` - Clean cache and processed files
* `make test` - Run tests

## Project Structure
```
data_engineering_project/
├── docker/          # docker config
├── config/          # spark config
├── data/            # data storage
├── src/             # source code
└── tests/           # test files
```

### Environment Variables (`.env`)

```
SPARK_MASTER=spark://spark-master:7077
SPARK_LOCAL_IP=127.0.0.1
PYTHONPATH=/app
```

> These ensure the app connects to the Spark master correctly inside the Docker network.

## Output

The application creates a DataFrame and writes it to:
```
data/processed/output/
```
> All containers now run as **root** with shared `777` permissions on `/app/data`,
> ensuring reliable read/write access during Spark job execution.

## Development Notes

* The **`spark-app`** container uses **Python 3.8** (matching the Spark image) for compatibility.
* `data/` and `src/` directories are **mounted as volumes**, allowing live code editing without rebuilding.
* Spark UI is available at [http://localhost:8080](http://localhost:8080)

To modify your code:

```bash
# Edit your source files
vim src/app.py

# Re-run the application
make run
```
