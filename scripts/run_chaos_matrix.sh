#!/usr/bin/env bash
# Every chaos run that is reported in docs/RESULTS.md. Raw output of each run
# (pass or fail) lands in results/raw/chaos-NNN-<label>.txt.
set -u
cd "$(dirname "$0")/.."

run() {
  echo ">>> chaos $*"
  docker compose --profile chaos run --rm chaos "$@"
  echo ">>> exit=$?"
}

# The spec run: 10k jobs, 8 workers, kill -9 every ~2s, duplicate POSTs, cancels.
for _ in 1 2 3 4 5; do run --label clean; done

# Harder variants of the real system.
run --label clean-sigstop --stop-frac 0.3          # 30% of chaos events freeze a worker for 8s instead
run --label clean-sigstop --stop-frac 0.3
run --label repeatable-read --isolation repeatable_read

# Non-transactional side effect (effect written to a file before the DB commit).
run --label file-effect --effect-mode file
run --label file-effect-sigstop --effect-mode file --stop-frac 0.3 --jobs 3000

# Negative controls: each must FAIL. Smaller (3000 jobs) to keep the matrix short.
run --label ctl-no-fencing-killonly --faults no_fencing --jobs 3000
run --label ctl-no-fencing-sigstop --faults no_fencing --stop-frac 0.3 --jobs 3000
run --label ctl-naive-claim --faults naive_claim --jobs 3000
run --label ctl-check-then-insert --faults check_then_insert --jobs 3000
run --label ctl-check-then-insert-burst2 --faults check_then_insert --dup-burst 2 --jobs 3000
run --label ctl-worker-clock --faults worker_clock --jobs 3000
