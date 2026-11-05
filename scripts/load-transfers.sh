#!/usr/bin/env bash
#
# Sends transfers to the API at a constant rate with vegeta.
#
# Every request gets its own Idempotency-Key
#
# Usage: scripts/load-transfers.sh [rate] [duration]
#
# Environment:
#   API_URL     API address (default http://localhost:8080)
#   ACCOUNT_A   first account (default 9)
#   ACCOUNT_B   second account (default 10)
#   AMOUNT      amount of every transfer (default 1)

set -eu

rate=${1:-20}
duration=${2:-3m}

api_url=${API_URL:-http://localhost:8080}
account_a=${ACCOUNT_A:-9}
account_b=${ACCOUNT_B:-10}
amount=${AMOUNT:-1}

if ! command -v vegeta >/dev/null; then
	echo "vegeta is not installed: go install github.com/tsenart/vegeta/v12@latest" >&2
	exit 1
fi

body() {
	printf '{"from_account_id":%d,"to_account_id":%d,"amount":%d}' "$1" "$2" "$amount" | base64 -w0
}

a_to_b=$(body "$account_a" "$account_b")
b_to_a=$(body "$account_b" "$account_a")

# targets prints one vegeta target per line. vegeta reads them
targets() {
	local i=0 body key
	while true; do
		if ((i++ % 2)); then body=$b_to_a; else body=$a_to_b; fi

		# every read of this file returns a new UUID
		read -r key </proc/sys/kernel/random/uuid

		printf '{"method":"POST","url":"%s/v1/transfers","body":"%s","header":{"Content-Type":["application/json"],"Idempotency-Key":["%s"]}}\n' \
			"$api_url" "$body" "$key"
	done
}

echo "transfers between accounts $account_a and $account_b: $rate/s for $duration" >&2

targets | vegeta attack -lazy -format=json -rate="$rate/s" -duration="$duration" | vegeta report
