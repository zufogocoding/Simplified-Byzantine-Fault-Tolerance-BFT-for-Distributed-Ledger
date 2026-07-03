# Makefile cho PBFT Distributed Ledger

.PHONY: help build up down demo test clean

help:
	@echo "Cac lenh su dung:"
	@echo "  make build   - Build Docker images cho cac PBFT nodes"
	@echo "  make up      - Khoi chay cum mang 4 node trong nen Docker Compose"
	@echo "  make down    - Dung va xoa cac container Docker Compose"
	@echo "  make demo    - Chay toan bo demo tu dong qua run_demo.sh"
	@echo "  make test    - Chay unit tests va integration tests"
	@echo "  make clean   - Don dep database RocksDB, files log va cache"

build:
	podman-compose build

up:
	podman-compose up -d

down:
	podman-compose down

demo:
	./run_demo.sh

test:
	python tests/test_crypto.py
	python tests/integration_test.py

clean:
	rm -rf logs/* rocksdb/*
	find . -type d -name "__pycache__" -exec rm -rf {} +
