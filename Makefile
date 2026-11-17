.PHONY: help build sim sim-msgs test race determinism chaos linz soak bench lint fmt clean

SEEDS  ?= 500
STEPS  ?= 5000
NODES  ?= 5
SEED   ?= 1

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

build: ## Build the simulator into ./bin
	go build -o bin/cerberussim ./cmd/cerberussim

sim: ## Watch a cluster elect, replicate, partition, crash and recover
	go run ./cmd/cerberussim -seed=$(SEED) -nodes=$(NODES)

sim-msgs: ## Same, tracing every message delivery
	go run ./cmd/cerberussim -seed=$(SEED) -nodes=$(NODES) -msgs

test: ## Unit + scenario tests with the race detector
	go test -race ./...

determinism: ## D1-D4: same seed twice must produce identical state hashes
	go test -race -run TestDeterminism -count=1 ./test/harness

chaos: ## Randomized fault injection with invariant checking
	go test -run TestRandomized -count=1 ./test/chaos -seeds=$(SEEDS) -steps=$(STEPS)

linz: ## Linearizability checking - NOT YET IMPLEMENTED (needs the KV layer, step 6)
	@echo "not yet implemented: needs internal/store first (RAFT_DESIGN.md section 10, step 6)"
	@echo "the Porcupine model and history recording land in ./test/linz"
	@exit 1

soak: ## Long run - 10k seeds x 50k steps. Slow; nightly.
	go test -run TestRandomized -count=1 -timeout=6h ./test/chaos \
		-seeds=10000 -steps=50000

bench: ## Benchmarks against the PRD section 6 targets
	go test -run=XXX -bench=. -benchmem ./...

lint: ## go vet + staticcheck
	go vet ./...
	@command -v staticcheck >/dev/null 2>&1 && staticcheck ./... || \
		echo "staticcheck not installed: go install honnef.co/go/tools/cmd/staticcheck@latest"

fmt: ## Format
	gofmt -s -w .

clean: ## Remove build output and node data
	rm -rf bin/ data/ *.test *.out *.pprof
