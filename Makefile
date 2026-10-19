PY ?= python
DATA ?= data

.PHONY: help install test test-fast lint demo spark-local evaluate console up down logs clean

help:            ## tampilkan daftar perintah
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-12s %s\n", $$1, $$2}'

install:         ## install paket + dependensi dev/spark (virtualenv disarankan)
	$(PY) -m pip install -e ".[dev,spark,local,console]"

test:            ## seluruh test (termasuk Spark end-to-end)
	$(PY) -m pytest

test-fast:       ## test tanpa Spark
	$(PY) -m pytest -m "not spark"

lint:
	ruff check src tests airflow apps

demo: clean      ## demo 1 menit TANPA Docker: simulasi -> pipeline lite -> evaluasi
	fraud-simulate --mode batch --sink files --out $(DATA)/landing --labels $(DATA)/labels/labels.jsonl
	fraud-lite --landing $(DATA)/landing --checkpoint $(DATA)/checkpoints/lite
	fraud-evaluate --labels $(DATA)/labels/labels.jsonl

spark-local: clean ## job Spark Structured Streaming asli (sumber file, tanpa Kafka)
	fraud-simulate --mode batch --sink files --out $(DATA)/landing --labels $(DATA)/labels/labels.jsonl
	fraud-stream --available-now
	fraud-evaluate --labels $(DATA)/labels/labels.jsonl

evaluate:        ## bandingkan hasil warehouse dengan label ground truth
	fraud-evaluate --labels $(DATA)/labels/labels.jsonl

console:         ## buka Fraud Investigation Console (Streamlit)
	streamlit run apps/investigation_console/app.py

up:              ## jalankan stack lengkap (Kafka, Spark, Postgres, Airflow, Metabase, Console)
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f spark-stream

clean:
	rm -rf $(DATA)
