COMPOSE := docker compose
RUN_DATE ?=

.DEFAULT_GOAL := help
.PHONY: help up down clean reset-dwh seed etl etl-rerun simulate analytics scd-demo psql-oltp psql-dwh bi logs ps all

help: ## Liệt kê các lệnh
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

up: ## Build image & khởi động PostgreSQL (OLTP, DWH) + Spark cluster
	$(COMPOSE) up -d --build --wait oltp-db dwh-db spark-master spark-worker

seed: ## Sinh dữ liệu mock vào OLTP (xóa dữ liệu cũ)
	$(COMPOSE) run --rm --build datagen python generate_data.py --reset

etl: ## Chạy pipeline OLTP -> OLAP trên Spark (RUN_DATE=YYYY-MM-DD tùy chọn)
	$(COMPOSE) exec spark-master /opt/submit.sh $(if $(RUN_DATE),--run-date $(RUN_DATE))

etl-rerun: ## Chạy lại transform/load từ snapshot bronze đã có (không đọc OLTP)
	$(COMPOSE) exec spark-master /opt/submit.sh --skip-extract $(if $(RUN_DATE),--run-date $(RUN_DATE))

simulate: ## Mô phỏng 1 ngày kinh doanh mới (đơn mới, khách chuyển nhà, đổi giá...)
	$(COMPOSE) run --rm datagen python simulate_changes.py

analytics: ## Chạy các truy vấn phân tích mẫu trên DWH
	$(COMPOSE) exec -T dwh-db sh -c 'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB" -f /sql/analytics.sql'

scd-demo: ## Xem lịch sử SCD Type 2 của khách hàng / sản phẩm
	$(COMPOSE) exec -T dwh-db sh -c 'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB" -f /sql/scd2_demo.sql'

psql-oltp: ## Mở psql vào OLTP
	$(COMPOSE) exec oltp-db sh -c 'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB"'

psql-dwh: ## Mở psql vào DWH
	$(COMPOSE) exec dwh-db sh -c 'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB"'

bi: ## Khởi động Metabase (http://localhost:3000)
	$(COMPOSE) --profile bi up -d metabase

logs: ## Xem log Spark
	$(COMPOSE) logs -f spark-master spark-worker

ps: ## Trạng thái các container
	$(COMPOSE) ps

all: up seed etl ## Dựng hạ tầng + sinh dữ liệu + chạy ETL

down: ## Dừng các container (giữ dữ liệu)
	$(COMPOSE) --profile bi --profile tools down

reset-dwh: ## Xóa sạch kho dữ liệu (DWH + bronze), giữ nguyên OLTP — dùng sau khi seed lại
	$(COMPOSE) rm -sf dwh-db
	docker volume rm -f data-modeling_dwh-data
	$(COMPOSE) exec spark-master sh -c 'rm -rf /opt/lake/bronze'
	$(COMPOSE) up -d --wait dwh-db

clean: ## Dừng và XÓA toàn bộ dữ liệu (volumes)
	$(COMPOSE) --profile bi --profile tools down -v
