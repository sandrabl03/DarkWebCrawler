export MONGO_URI = mongodb://192.168.56.1:27017
export NEO_URI = bolt://192.168.56.1:7688
export PYTHONPATH = .

AHMIA_SCRIPT = src/crawler/ahmia_scraper.py

run-docker:
	docker restart neo4j && docker restart mongodb

run-ahmia-scraper:
	python3 $(AHMIA_SCRIPT)

run-seed-loader:
	python3 -m src.crawler.seed_loader

run-tor-controller:
	python3 -m src.crawler.tor_controller