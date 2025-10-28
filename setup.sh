#!/bin/bash

echo "Setting up Data Engineering Project..."

mkdir -p data/raw data/processed
chmod -R 777 data

touch data/raw/.gitkeep
touch data/processed/.gitkeep

if [ ! -f .env ]; then
    echo "Creating .env file..."
    cat > .env << EOF
SPARK_LOCAL_IP=127.0.0.1
PYTHONPATH=/app
EOF
fi

echo "✓ Project structure created"
echo "✓ Directory permissions set (777)"
echo "✓ Setup complete!"
echo ""
echo "Next steps:"
echo "  1. Run 'make build' to build Docker image"
echo "  2. Run 'make up' to start Spark cluster"
echo "  3. Run 'make run' to execute the application"