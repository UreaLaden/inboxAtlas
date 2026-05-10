.PHONY: fmt lint build test run coverage coverage-pkg coverage-func coverage-total sync classify-run classify-infer gen-reports gen-classify-reports gen-promote-unknowns pipeline gen-subject-eval validate-subject-eval test-scripts
CMD     := ./cmd/inboxatlas
SUMMARY_PROVIDER := ./cmd/openai-summary-provider
INFERENCE_PROVIDER := ./cmd/ai-inference-provider
GO ?= go
GO_TEST_ENV := env GOCACHE=/tmp/inboxatlas-gocache GOTMPDIR=/tmp/inboxatlas-gotmp
GO_TEST_PREP := mkdir -p /tmp/inboxatlas-gocache /tmp/inboxatlas-gotmp
GO_TEST_FLAGS :=

REPORTS_DIR := ./.ai/references/reports/out

ifeq ($(OS),Windows_NT)
BINARY := inboxatlas.exe
SUMMARY_PROVIDER_BINARY := openai-summary-provider.exe
AI_INFERENCE_BINARY := ai-inference-provider.exe
GO_TEST_FLAGS += -work
else
BINARY := inboxatlas
SUMMARY_PROVIDER_BINARY := openai-summary-provider
AI_INFERENCE_BINARY := ai-inference-provider
endif

# Format all Go source files.
fmt:
	$(GO) fmt ./...

# Run the golangci-lint linter suite.
lint: fmt
	golangci-lint run ./...

# Build the inboxatlas, ia, openai-summary-provider, and ai-inference-provider binaries.
build:
	@$(GO) build -o $(BINARY) $(CMD)
	@$(GO) build -o ia $(CMD)
	@$(GO) build -o $(SUMMARY_PROVIDER_BINARY) $(SUMMARY_PROVIDER)
	@$(GO) build -o $(AI_INFERENCE_BINARY) $(INFERENCE_PROVIDER)
	@echo "Build complete!"

# Run all tests.
test:
	@$(GO_TEST_PREP)
	$(GO_TEST_ENV) $(GO) test $(GO_TEST_FLAGS) ./...

test-verbose:
	$(GO_TEST_ENV) $(GO) test ./... -v

# Generate coverage profile (prerequisite for all coverage-* targets).
coverage:
	@$(GO_TEST_PREP)
	$(GO_TEST_ENV) $(GO) test $(GO_TEST_FLAGS) -coverprofile=coverage.out ./...

# Function-level breakdown (includes total line at the bottom).
coverage-func: coverage
	@echo ""
	@echo "--- Function-level coverage ---"
	$(GO) tool cover -func=coverage.out

# Total repository coverage as a single summary line.
coverage-total: coverage
	@$(GO) tool cover -func=coverage.out | grep "^total:"

# Build and run the inboxatlas binary.
run: build
	./$(BINARY)

ACCOUNT?= 
gen-reports-csv:
	@./$(BINARY) report domains --account $(ACCOUNT) --format csv > .ai/references/reports/out/domains.csv
	@./$(BINARY) report senders --account $(ACCOUNT) --format csv > .ai/references/reports/out/senders.csv
	@./$(BINARY) report subjects --account $(ACCOUNT) --format csv > .ai/references/reports/out/subjects.csv
	@./$(BINARY) report volume --account $(ACCOUNT) --format csv > .ai/references/reports/out/volume.csv

gen-reports-json:
	@./$(BINARY) report domains --account $(ACCOUNT) --format json > .ai/references/reports/out/domains.json
	@./$(BINARY) report senders --account $(ACCOUNT) --format json > .ai/references/reports/out/senders.json
	@./$(BINARY) report subjects --account $(ACCOUNT) --format json > .ai/references/reports/out/subjects.json
	@./$(BINARY) report volume --account $(ACCOUNT) --format json > .ai/references/reports/out/volume.json


gen-promote-commands:
	@comm -23 \
		<(./$(BINARY) classify suggestions --account $(ACCOUNT) | awk 'NR>1 {print $$1 ":" $$2}' | sort) \
		<(./$(BINARY)  classify seeds list --account $(ACCOUNT) | awk 'NR>1 {print $$2 ":" $$3}' | sort) \
	  | while IFS=: read ptype pvalue; do \
			echo "./$(BINARY) classify promote --account $(ACCOUNT) --pattern-type $$ptype --pattern-value $$pvalue --category <CATEGORY>"; \
		done


LIMIT ?= 0
BATCH_SIZE ?= 10
sync:
	@./$(BINARY) sync gmail --account $(ACCOUNT) --limit $(LIMIT)

# Run pattern-based classification for one account.
classify-run:
	@./$(BINARY) classify run --account $(ACCOUNT)

# Run AI-assisted inference for one account using the bundled inference provider.
classify-infer:
	@./$(BINARY) classify infer \
		--account $(ACCOUNT) \
		--provider-command ./$(AI_INFERENCE_BINARY) \
		--batch-size $(BATCH_SIZE)

# Generate classification reference reports: suggestions (inferred categories per domain)
# and classified messages (ground-truth per-message category assignments).
# Used as input for filling in promote.txt category placeholders.
# Usage: make gen-classify-reports ACCOUNT=acr
gen-classify-reports:
	@./$(BINARY) classify suggestions --account $(ACCOUNT) --format json > .ai/references/reports/out/suggestions.json
	@./$(BINARY) classify messages --account $(ACCOUNT) --format json --limit 0 > .ai/references/reports/out/classified-messages.json
	@$(MAKE) --no-print-directory gen-promote-commands ACCOUNT=$(ACCOUNT) > .ai/references/reports/out/promote.txt

# Extract unresolved promote commands (still containing <CATEGORY>) from promote.txt
# into a standalone file for manual review and gap resolution.
# Usage: make gen-promote-unknowns
gen-promote-unknowns:
	@grep "<CATEGORY>" .ai/references/reports/promote.txt | grep "classify promote" > .ai/references/reports/out/promote-unknowns.txt || true
	@echo "$$(grep -c 'classify promote' .ai/references/reports/out/promote-unknowns.txt) unresolved entries → .ai/references/reports/out/promote-unknowns.txt"

# Generate both CSV and JSON report sets in one shot.
gen-reports: gen-reports-csv gen-reports-json


# Run the full pipeline for one account: sync → classify → infer → reports.
pipeline: sync classify-run classify-infer gen-reports-csv

OWNER_EMAIL ?=
summarize-reports:
	@./$(BINARY) report summarize \
		--reports-dir $(REPORTS_DIR) \
		--owner-email $(OWNER_EMAIL) \
		--provider-command ./$(SUMMARY_PROVIDER_BINARY) \
		--prompt-file ./.claude/commands/summarizeReport.md

gen-export-html: 
	@./$(BINARY) report export \
		--reports-dir $(REPORTS_DIR) \
		--output-dir $(REPORTS_DIR) \
		--format html \
		--owner-email $(OWNER_EMAIL) \
		--summary-file $(REPORTS_DIR)/summary.md

gen-export-excel: 
	@./$(BINARY) report export \
		--reports-dir $(REPORTS_DIR) \
		--output-dir $(REPORTS_DIR) \
		--format excel \
		--owner-email $(OWNER_EMAIL) \
		--summary-file $(REPORTS_DIR)/summary.md

gen-exports: gen-export-html gen-export-excel

# Generate a classify subject-eval command from a labeled CSV export.
#
# The CSV must have a header row with a "subject" column.
# Strips RE:/FW:/Fwd:/AW: prefixes (mirrors NormalizeSubject), tokenizes on
# whitespace + punctuation, lowercases, filters stop words and short tokens,
# then emits the top TOP_N tokens as --include flags on a ready-to-run command.
#
# Usage:
#   make gen-subject-eval ACCOUNT=acr CSV=.ai/references/reports/out/acr/payment_needed.csv CATEGORY=client
#   make gen-subject-eval ACCOUNT=acr CSV=path/to/labels.csv CATEGORY=vendor TOP_N=15
#
# Optional overrides:
#   TOP_N          — number of top tokens to include (default: 12)
#   EXCLUDE_CAT    — --exclude-category value(s), space-separated (default: empty)
#   MATCHED_ONLY   — pass --matched-only flag (default: true)
CSV ?=
CATEGORY ?= client
TOP_N ?= 12
EXCLUDE_CAT ?=
MATCHED_ONLY ?= true

PYTHON ?= $(shell python3 --version >/dev/null 2>&1 && echo python3 || echo python)

# Run unit tests for .ai/scripts Python utilities.
test-scripts:
	@$(PYTHON) .ai/scripts/test_validate_subject_eval.py

gen-subject-eval:
	@if [ -z "$(CSV)" ]; then \
		echo "ERROR: CSV is required. Usage: make gen-subject-eval ACCOUNT=<acct> CSV=<path> CATEGORY=<cat>"; \
		exit 1; \
	fi
	@$(PYTHON) .ai/scripts/gen_subject_eval.py \
		--csv "$(CSV)" \
		--account "$(ACCOUNT)" \
		--category "$(CATEGORY)" \
		--top-n $(TOP_N) \
		$(foreach cat,$(EXCLUDE_CAT),--exclude-cat $(cat)) \
		$(if $(filter false,$(MATCHED_ONLY)),--no-matched-only,)

# Validate subject-eval JSON recall against a labeled CSV export.
#
# Required:
#   CSV            — labeled message export (labeled subset, not full mailbox)
#   JSON           — subject-eval output from classify subject-eval --format json
#
# Optional:
#   LABEL          — version label printed in provenance header (e.g. payment_needed_v2)
#   EXPECTED_CAT   — InboxAtlas category expected for this Gmail label (enables conflict check)
#   COMPARE_JSON   — baseline JSON to compare against (e.g. seed run); shows recovered subjects
#   DOMAIN_ANALYSIS — set to true to add domain breakdown of missed messages
#   DEBUG_SUBJECTS  — set to true to show normalization diagnostics for each missed subject
#
# Usage:
#   make validate-subject-eval CSV=... JSON=... LABEL=payment_needed_v2
#   make validate-subject-eval CSV=... JSON=... COMPARE_JSON=..._seed.json
#   make validate-subject-eval CSV=... JSON=... EXPECTED_CAT=vendor DOMAIN_ANALYSIS=true
JSON ?=
LABEL ?=
EXPECTED_CAT ?=
COMPARE_JSON ?=
DOMAIN_ANALYSIS ?= false
DEBUG_SUBJECTS ?= false

validate-subject-eval:
	@if [ -z "$(CSV)" ]; then \
		echo "ERROR: CSV is required. Usage: make validate-subject-eval CSV=<path> JSON=<path>"; \
		exit 1; \
	fi
	@if [ -z "$(JSON)" ]; then \
		echo "ERROR: JSON is required. Usage: make validate-subject-eval CSV=<path> JSON=<path>"; \
		exit 1; \
	fi
	@$(PYTHON) .ai/scripts/validate_subject_eval.py \
		--csv "$(CSV)" \
		--json "$(JSON)" \
		$(if $(LABEL),--label "$(LABEL)",) \
		$(if $(ACCOUNT),--account "$(ACCOUNT)",) \
		$(if $(EXPECTED_CAT),--expected-category "$(EXPECTED_CAT)",) \
		$(if $(COMPARE_JSON),--compare-json "$(COMPARE_JSON)",) \
		$(if $(filter true,$(DOMAIN_ANALYSIS)),--domain-analysis,) \
		$(if $(filter true,$(DEBUG_SUBJECTS)),--debug-subjects,)