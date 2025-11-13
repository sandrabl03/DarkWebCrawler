AHMIA_SCRIPT = src/crawler/ahmia_scraper.py

run-docker:
	docker restart neo4j && docker restart mongodb

run-ahmia-scraper:
	python3 $(AHMIA_SCRIPT)

run-seed-loader:
	PYTHONPATH=. python3 -m src.crawler.seed_loader


run-tor-controller:
	PYTHONPATH=. python3 -m src.crawler.tor_controller

