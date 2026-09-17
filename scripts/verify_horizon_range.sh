#!/usr/bin/env bash
# Verify a contiguous Horizon archive range without serializing PoH work.
#
# Acceptance is the conjunction of three checks:
#   1. Every archive matches its standard SHA-256 sidecar.
#   2. One ordered scan verifies coverage, decoding, and all archive boundaries.
#   3. Each archive independently recomputes every block's PoH in parallel.
set -Eeuo pipefail

umask 027
export LC_ALL=C
export PATH=/usr/bin:/bin

usage() {
    echo "usage: $0 VERIFY_ARCHIVE ARCHIVE_DIR STATE_DIR START_EPOCH END_EPOCH [JOBS] [THREADS_PER_JOB] [CHAIN_THREADS]" >&2
    exit 2
}

[[ $# -ge 5 && $# -le 8 ]] || usage

verifier=$1
archive_dir=$2
state_dir=$3
start_epoch=$4
end_epoch=$5
jobs=${6:-4}
threads_per_job=${7:-12}
chain_threads=${8:-8}

for value in "$start_epoch" "$end_epoch" "$jobs" "$threads_per_job" "$chain_threads"; do
    [[ $value =~ ^[0-9]+$ ]] || usage
done
((start_epoch <= end_epoch)) || usage
((jobs >= 1 && jobs <= 64)) || usage
((threads_per_job >= 1 && threads_per_job <= 256)) || usage
((chain_threads >= 1 && chain_threads <= 256)) || usage
[[ $verifier == /* && $archive_dir == /* && $state_dir == /* ]] || usage
[[ -f $verifier && -x $verifier && ! -L $verifier ]] || {
    echo "verifier must be an executable regular file, not a symlink: $verifier" >&2
    exit 2
}
[[ -d $archive_dir && ! -L $archive_dir ]] || {
    echo "archive directory must be a regular directory, not a symlink: $archive_dir" >&2
    exit 2
}
if [[ -e $state_dir && (! -d $state_dir || -L $state_dir) ]]; then
    echo "state path must be a regular directory, not a symlink: $state_dir" >&2
    exit 2
fi

mkdir -p "$state_dir/logs" "$state_dir/receipts" "$state_dir/tmp"
for directory in "$state_dir" "$state_dir/logs" "$state_dir/receipts" "$state_dir/tmp"; do
    [[ -d $directory && ! -L $directory ]] || {
        echo "audit state directory must not be a symlink: $directory" >&2
        exit 2
    }
done
archive_dir=$(realpath "$archive_dir")
state_dir=$(realpath "$state_dir")
case "$state_dir/" in
    "$archive_dir/"*)
        echo "state directory must be outside the archive directory" >&2
        exit 2
        ;;
esac

verifier_sha=$(sha256sum -- "$verifier" | awk '{print $1}')
script_sha=$(sha256sum -- "$0" | awk '{print $1}')
children=()

cleanup_children() {
    local status=$?
    if ((status != 0 && ${#children[@]} != 0)); then
        kill "${children[@]}" 2>/dev/null || true
        wait "${children[@]}" 2>/dev/null || true
    fi
}
trap cleanup_children EXIT

while true; do
    missing=()
    for ((epoch = start_epoch; epoch <= end_epoch; epoch++)); do
        archive="$archive_dir/epoch-$epoch.jet"
        sidecar="$archive.sha256"
        if [[ ! -f $archive || -L $archive || ! -f $sidecar || -L $sidecar ]]; then
            missing+=("$epoch")
        fi
    done
    if ((${#missing[@]} == 0)); then
        break
    fi
    printf '[%s] waiting for archive pairs; missing=%s\n' \
        "$(date -u +%FT%TZ)" "${missing[*]}"
    sleep 300
done

echo "[$(date -u +%FT%TZ)] all archive pairs present; verifying SHA-256 sidecars"
manifest_tmp="$state_dir/tmp/manifest.$$.tmp"
: >"$manifest_tmp"
for ((epoch = start_epoch; epoch <= end_epoch; epoch++)); do
    name="epoch-$epoch.jet"
    archive="$archive_dir/$name"
    sidecar="$archive.sha256"
    line=$(<"$sidecar")
    if [[ ! $line =~ ^([0-9a-f]{64})\ \ (epoch-[0-9]+\.jet)$ ]] \
        || [[ ${BASH_REMATCH[2]:-} != "$name" ]]; then
        echo "invalid SHA-256 sidecar format: $sidecar" >&2
        exit 1
    fi
    expected=${BASH_REMATCH[1]}
    actual=$(sha256sum -- "$archive" | awk '{print $1}')
    if [[ $actual != "$expected" ]]; then
        echo "SHA-256 mismatch for $archive: expected=$expected actual=$actual" >&2
        exit 1
    fi
    printf '%s  %s\n' "$actual" "$name" >>"$manifest_tmp"
done
mv -f -- "$manifest_tmp" "$state_dir/archive-manifest.sha256"
manifest_sha=$(sha256sum -- "$state_dir/archive-manifest.sha256" | awk '{print $1}')
echo "[$(date -u +%FT%TZ)] all sidecars verified; manifest_sha256=$manifest_sha"

run_chain() {
    local receipt="$state_dir/receipts/ordered-chain.ok"
    local prior_manifest="" prior_verifier="" prior_script=""
    if [[ -f $receipt ]]; then
        read -r prior_manifest prior_verifier prior_script <"$receipt" || true
        if [[ $prior_manifest == "$manifest_sha" \
            && $prior_verifier == "$verifier_sha" \
            && $prior_script == "$script_sha" ]]; then
            echo "[$(date -u +%FT%TZ)] ordered-chain receipt matches; skipping"
            return 0
        fi
    fi

    local paths=()
    local epoch
    for ((epoch = start_epoch; epoch <= end_epoch; epoch++)); do
        paths+=("$archive_dir/epoch-$epoch.jet")
    done
    local log_tmp="$state_dir/tmp/ordered-chain.$$.tmp"
    if nice -n 19 ionice -c 3 "$verifier" --chain "${paths[@]}" \
        --threads "$chain_threads" >"$log_tmp" 2>&1; then
        mv -f -- "$log_tmp" "$state_dir/logs/ordered-chain.log"
        local receipt_tmp="$state_dir/tmp/ordered-chain-receipt.$$.tmp"
        printf '%s %s %s\n' "$manifest_sha" "$verifier_sha" "$script_sha" >"$receipt_tmp"
        mv -f -- "$receipt_tmp" "$receipt"
        echo "[$(date -u +%FT%TZ)] ordered-chain verification passed"
        return 0
    fi
    mv -f -- "$log_tmp" "$state_dir/logs/ordered-chain.failed.$(date -u +%Y%m%dT%H%M%SZ).log"
    echo "ordered-chain verification failed" >&2
    return 1
}

run_full_epoch() {
    local epoch=$1
    local archive="$archive_dir/epoch-$epoch.jet"
    local archive_sha
    archive_sha=$(awk -v name="epoch-$epoch.jet" '$2 == name { print $1 }' \
        "$state_dir/archive-manifest.sha256")
    [[ $archive_sha =~ ^[0-9a-f]{64}$ ]] || {
        echo "manifest entry missing for epoch $epoch" >&2
        return 1
    }

    local receipt="$state_dir/receipts/epoch-$epoch.full.ok"
    local prior_archive="" prior_verifier="" prior_script=""
    if [[ -f $receipt ]]; then
        read -r prior_archive prior_verifier prior_script <"$receipt" || true
        if [[ $prior_archive == "$archive_sha" \
            && $prior_verifier == "$verifier_sha" \
            && $prior_script == "$script_sha" ]]; then
            echo "[$(date -u +%FT%TZ)] epoch $epoch full receipt matches; skipping"
            return 0
        fi
    fi

    local log_tmp="$state_dir/tmp/epoch-$epoch.$$.tmp"
    echo "[$(date -u +%FT%TZ)] epoch $epoch full PoH verification started"
    if nice -n 19 ionice -c 3 "$verifier" "$archive" --full --internal-full \
        --threads "$threads_per_job" >"$log_tmp" 2>&1; then
        mv -f -- "$log_tmp" "$state_dir/logs/epoch-$epoch.full.log"
        local receipt_tmp="$state_dir/tmp/epoch-$epoch-receipt.$$.tmp"
        printf '%s %s %s\n' "$archive_sha" "$verifier_sha" "$script_sha" >"$receipt_tmp"
        mv -f -- "$receipt_tmp" "$receipt"
        echo "[$(date -u +%FT%TZ)] epoch $epoch full PoH verification passed"
        return 0
    fi
    mv -f -- "$log_tmp" "$state_dir/logs/epoch-$epoch.full.failed.$(date -u +%Y%m%dT%H%M%SZ).log"
    echo "epoch $epoch full PoH verification failed" >&2
    return 1
}

run_full_pool() {
    local active=()
    local failed=0
    local epoch pid first
    for ((epoch = start_epoch; epoch <= end_epoch; epoch++)); do
        run_full_epoch "$epoch" &
        pid=$!
        children+=("$pid")
        active+=("$pid")
        if ((${#active[@]} >= jobs)); then
            first=${active[0]}
            if ! wait "$first"; then
                failed=1
            fi
            active=("${active[@]:1}")
            ((failed == 0)) || break
        fi
    done
    for pid in "${active[@]}"; do
        if ! wait "$pid"; then
            failed=1
        fi
    done
    return "$failed"
}

run_chain &
chain_pid=$!
children+=("$chain_pid")

failed=0
if ! run_full_pool; then
    failed=1
fi
if ! wait "$chain_pid"; then
    failed=1
fi
((failed == 0)) || exit 1

echo "[$(date -u +%FT%TZ)] verification passes complete; rechecking archive SHA-256 values"
while read -r expected name; do
    [[ $name =~ ^epoch-[0-9]+\.jet$ ]] || {
        echo "invalid archive manifest entry: $name" >&2
        exit 1
    }
    actual=$(sha256sum -- "$archive_dir/$name" | awk '{print $1}')
    if [[ $actual != "$expected" ]]; then
        echo "archive changed during verification: $name expected=$expected actual=$actual" >&2
        exit 1
    fi
done <"$state_dir/archive-manifest.sha256"

echo "[$(date -u +%FT%TZ)] ACCEPTED: SHA-256, ordered-chain, and full per-epoch PoH checks passed for epochs $start_epoch-$end_epoch"
