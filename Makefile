.PHONY: help proto proto-clean proto-check proto-update tools-check build test clean all

# Цвета для вывода
GREEN  := \033[0;32m
YELLOW := \033[0;33m
RED    := \033[0;31m
NC     := \033[0m # No Color

# Переменные
PROTO_DIR := protobufs
PROTO_MESHTASTIC_DIR := $(PROTO_DIR)/meshtastic
GEN_DIR := internal/meshtastic
GO_MODULE := github.com/skrashevich/go-meshtastic-serial2tcp
TEMP_GEN_DIR := github.com/meshtastic/go/generated

# Требуемые версии инструментов
REQUIRED_PROTOC_GEN_GO := v1.36.11

help: ## Показать эту справку
	@echo "$(GREEN)Доступные команды:$(NC)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(YELLOW)%-20s$(NC) %s\n", $$1, $$2}'

tools-check: ## Проверить наличие необходимых инструментов
	@echo "$(GREEN)Проверка инструментов...$(NC)"
	@which protoc > /dev/null || (echo "$(RED)Ошибка: protoc не установлен$(NC)" && exit 1)
	@which protoc-gen-go > /dev/null || (echo "$(RED)Ошибка: protoc-gen-go не установлен$(NC)" && echo "Установите: go install google.golang.org/protobuf/cmd/protoc-gen-go@latest" && exit 1)
	@echo "  protoc version: $$(protoc --version)"
	@echo "  protoc-gen-go version: $$(protoc-gen-go --version)"
	@CURRENT_VERSION=$$(protoc-gen-go --version | awk '{print $$2}'); \
	if [ "$$CURRENT_VERSION" != "$(REQUIRED_PROTOC_GEN_GO)" ]; then \
		echo "$(YELLOW)Предупреждение: рекомендуется protoc-gen-go $(REQUIRED_PROTOC_GEN_GO), установлена $$CURRENT_VERSION$(NC)"; \
		echo "Обновите: go install google.golang.org/protobuf/cmd/protoc-gen-go@$(REQUIRED_PROTOC_GEN_GO)"; \
	fi

proto-check: tools-check ## Проверить наличие protobuf файлов
	@echo "$(GREEN)Проверка protobuf файлов...$(NC)"
	@if [ ! -d "$(PROTO_DIR)" ]; then \
		echo "$(RED)Ошибка: директория $(PROTO_DIR) не найдена$(NC)"; \
		echo "Выполните: git submodule update --init --recursive"; \
		exit 1; \
	fi
	@if [ ! -d "$(PROTO_MESHTASTIC_DIR)" ]; then \
		echo "$(RED)Ошибка: директория $(PROTO_MESHTASTIC_DIR) не найдена$(NC)"; \
		exit 1; \
	fi
	@PROTO_COUNT=$$(find $(PROTO_MESHTASTIC_DIR) -name "*.proto" -type f | wc -l); \
	echo "  Найдено $$PROTO_COUNT proto файлов"

proto-update: ## Обновить submodule с protobuf файлами
	@echo "$(GREEN)Обновление submodule protobufs...$(NC)"
	@git submodule update --init --recursive
	@cd $(PROTO_DIR) && git pull origin master
	@echo "$(GREEN)Submodule обновлён до:$(NC)"
	@cd $(PROTO_DIR) && git log -1 --oneline

proto-clean: ## Удалить сгенерированные protobuf файлы
	@echo "$(YELLOW)Удаление сгенерированных файлов...$(NC)"
	@rm -f $(GEN_DIR)/*.pb.go
	@rm -rf $(TEMP_GEN_DIR)
	@echo "$(GREEN)Готово$(NC)"

proto: proto-check ## Сгенерировать Go код из protobuf файлов
	@echo "$(GREEN)Генерация protobuf файлов...$(NC)"
	@mkdir -p $(TEMP_GEN_DIR)

	@echo "  Генерация meshtastic proto файлов..."
	@protoc \
		--proto_path=$(PROTO_DIR) \
		--go_out=. \
		$(PROTO_MESHTASTIC_DIR)/*.proto

	@echo "  Генерация nanopb.proto..."
	@protoc \
		--proto_path=$(PROTO_DIR) \
		--go_out=. \
		$(PROTO_DIR)/nanopb.proto

	@echo "  Перемещение сгенерированных файлов..."
	@mkdir -p $(GEN_DIR)
	@mv $(TEMP_GEN_DIR)/*.pb.go $(GEN_DIR)/
	@rm -rf github.com

	@GENERATED_COUNT=$$(ls -1 $(GEN_DIR)/*.pb.go 2>/dev/null | wc -l); \
	echo "$(GREEN)Сгенерировано $$GENERATED_COUNT файлов в $(GEN_DIR)$(NC)"

	@echo "$(GREEN)Проверка package name...$(NC)"
	@head -7 $(GEN_DIR)/admin.pb.go | grep "package generated" || \
		(echo "$(RED)Ошибка: неверное имя package$(NC)" && exit 1)

	@echo "$(GREEN)✓ Генерация завершена успешно$(NC)"

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null | sed 's/^v//')
LDFLAGS ?= -ldflags "-X main.version=$(VERSION)"

build: ## Собрать проект
	@echo "$(GREEN)Сборка проекта...$(NC)"
	@go build -v $(LDFLAGS) -o go-meshtastic-serial2tcp .
	@echo "$(GREEN)✓ Сборка завершена (version=$(VERSION))$(NC)"

run: ## Запустить с версией из git describe
	@go run $(LDFLAGS) .

test: ## Запустить тесты
	@echo "$(GREEN)Запуск тестов...$(NC)"
	@go test -v ./...

clean: proto-clean ## Очистить всё (сгенерированные файлы и бинарники)
	@echo "$(YELLOW)Очистка...$(NC)"
	@rm -f go-meshtastic-serial2tcp
	@rm -f go-meshtastic-serial2tcp.exe
	@go clean
	@echo "$(GREEN)✓ Готово$(NC)"

all: proto build ## Сгенерировать protobuf и собрать проект
	@echo "$(GREEN)✓ Всё готово!$(NC)"
