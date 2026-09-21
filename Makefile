.PHONY: help spark-up spark-ps spark-logs spark-down spark-submit-smoke

SPARK_MASTER_URL ?= spark://spark-master:7077
SPARK_MASTER_SERVICE ?= spark-master
SPARK_JOB ?= /opt/skypoints/jobs/spark/smoke_test.py

help:
	@echo "SkyPoints local Spark commands"
	@echo ""
	@echo "  make spark-up             Start Spark master and worker"
	@echo "  make spark-ps             Show Spark service status"
	@echo "  make spark-logs           Follow Spark service logs"
	@echo "  make spark-submit-smoke   Write and read a local Delta table"
	@echo "  make spark-down           Stop Spark services"

spark-up:
	docker compose up -d spark-master spark-worker

spark-ps:
	docker compose ps

spark-logs:
	docker compose logs -f spark-master spark-worker

spark-submit-smoke:
	docker compose exec -T $(SPARK_MASTER_SERVICE) \
		spark-submit \
		--master $(SPARK_MASTER_URL) \
		--conf spark.executorEnv.PYTHONPATH=/opt/skypoints/jobs/spark \
		$(SPARK_JOB)

spark-down:
	docker compose down
