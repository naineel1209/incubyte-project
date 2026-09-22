.PHONY: help spark-up spark-ps spark-logs spark-down spark-submit-smoke spark-submit-member-profile spark-submit-member-exports spark-submit-redemption-ingestion spark-sql-validation spark-test-validations

SPARK_MASTER_URL ?= spark://spark-master:7077
SPARK_MASTER_SERVICE ?= spark-master
SPARK_JOB ?= /opt/skypoints/jobs/spark/smoke_test.py
SPARK_MEMBER_PROFILE_JOB ?= /opt/skypoints/jobs/spark/member_profile_ingestion.py
SPARK_MEMBER_EXPORT_JOB ?= /opt/skypoints/jobs/spark/member_profile_exports.py
SPARK_REDEMPTION_JOB ?= /opt/skypoints/jobs/spark/redemption_ingestion.py

help:
	@echo "SkyPoints local Spark commands"
	@echo ""
	@echo "  make spark-up             Start Spark master and worker"
	@echo "  make spark-ps             Show Spark service status"
	@echo "  make spark-logs           Follow Spark service logs"
	@echo "  make spark-submit-smoke   Write and read a local Delta table"
	@echo "  make spark-submit-member-profile  Ingest member profile files"
	@echo "  make spark-submit-member-exports  Create country Delta targets"
	@echo "  make spark-submit-redemption-ingestion  Ingest redemption JSON feeds"
	@echo "  make spark-sql-validation     Query quarantined validation results"
	@echo "  make spark-test-validations  Run automated validation tests"
	@echo "  make spark-down           Stop Spark services"

spark-up:
	docker compose up -d --build spark-master spark-worker

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

spark-submit-member-profile:
	docker compose exec -T $(SPARK_MASTER_SERVICE) \
		spark-submit \
		--master $(SPARK_MASTER_URL) \
		--conf spark.executorEnv.PYTHONPATH=/opt/skypoints/jobs/spark \
		$(SPARK_MEMBER_PROFILE_JOB)

spark-submit-member-exports:
	docker compose exec -T $(SPARK_MASTER_SERVICE) \
		spark-submit \
		--master $(SPARK_MASTER_URL) \
		--conf spark.executorEnv.PYTHONPATH=/opt/skypoints/jobs/spark \
		$(SPARK_MEMBER_EXPORT_JOB)

spark-submit-redemption-ingestion:
	docker compose exec -T $(SPARK_MASTER_SERVICE) \
		spark-submit \
		--master $(SPARK_MASTER_URL) \
		--conf spark.executorEnv.PYTHONPATH=/opt/skypoints/jobs/spark \
		$(SPARK_REDEMPTION_JOB)

spark-sql-validation:
	docker compose exec -T $(SPARK_MASTER_SERVICE) \
		spark-sql \
		--master 'local[2]' \
		-f /opt/skypoints/jobs/spark/sql/data_validation_queries.sql

spark-test-validations:
	docker compose exec -T $(SPARK_MASTER_SERVICE) \
		spark-submit \
		--master 'local[2]' \
		--conf spark.executorEnv.PYTHONPATH=/opt/skypoints/jobs/spark \
		/opt/skypoints/jobs/spark/tests/test_data_validations.py

spark-down:
	docker compose down
