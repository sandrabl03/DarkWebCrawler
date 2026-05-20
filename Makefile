MONGO_URI ?= mongodb://localhost:27017
NEO_URI ?= bolt://localhost:7688
PYTHONPATH ?= .

export MONGO_URI
export NEO_URI
export PYTHONPATH

AHMIA_SCRIPT = src/crawler/ahmia_scraper.py


run-docker:
	docker restart neo4j && docker restart mongodb

run-ahmia-scraper:
	python3 $(AHMIA_SCRIPT)

run-seed-loader:
	python3 -m src.crawler.seed_loader

run-tor-controller:
	python3 -m src.crawler.tor_controller