#!/usr/bin/env bash
# Dispatch the sealed per-epoch plugin verifier only for archives that do not
# already have its exact receipt. This avoids re-hashing every verified local
# archive on every polling pass while preserving the verifier's receipt format.
set -Eeuo pipefail

umask 077
export LC_ALL=C
export PATH=/usr/bin:/bin

usage() {
    echo "usage: $0 VERIFIER HORIZON_PIPELINE ARCHIVE_DIR STATE_DIR START_EPOCH END_EPOCH EXPECTED_PIPELINE_SHA256 EXPECTED_VERIFIER_SHA256 [THREADS] [POLL_SECONDS]" >&2
    exit 2
}

[[ $# -ge 8 && $# -le 10 ]] || usage

verifier=$1
pipeline=$2
archive_dir=$3
state_dir=$4
start_epoch=$5
end_epoch=$6
expected_pipeline_sha=$7
expected_verifier_sha=$8
threads=${9:-16}
poll_seconds=${10:-300}

for value in "$start_epoch" "$end_epoch" "$threads" "$poll_seconds"; do
    [[ $value =~ ^[0-9]+$ ]] || usage
done
((start_epoch <= end_epoch)) || usage
((threads >= 1 && threads <= 256)) || usage
((poll_seconds >= 1 && poll_seconds <= 86400)) || usage
[[ $expected_pipeline_sha =~ ^[0-9a-f]{64}$ ]] || usage
[[ $expected_verifier_sha =~ ^[0-9a-f]{64}$ ]] || usage
[[ $verifier == /* && $pipeline == /* && $archive_dir == /* && $state_dir == /* ]] || usage

for executable in "$verifier" "$pipeline"; do
    [[ -f $executable && -x $executable && ! -L $executable ]] || {
        echo "expected an executable regular file, not a symlink: $executable" >&2
        exit 2
    }
done
[[ -d $archive_dir && ! -L $archive_dir ]] || {
    echo "archive directory must be a regular directory, not a symlink: $archive_dir" >&2
    exit 2
}
[[ -d $state_dir && ! -L $state_dir && -d $state_dir/receipts \
    && ! -L $state_dir/receipts ]] || {
    echo "plugin verification state paths must be regular directories" >&2
    exit 2
}

verifier=$(realpath "$verifier")
pipeline=$(realpath "$pipeline")
archive_dir=$(realpath "$archive_dir")
state_dir=$(realpath "$state_dir")

pipeline_sha=$(/usr/bin/sha256sum --binary "$pipeline" | cut -d' ' -f1)
verifier_sha=$(/usr/bin/sha256sum --binary "$verifier" | cut -d' ' -f1)
[[ $pipeline_sha == "$expected_pipeline_sha" ]] || {
    echo "horizon pipeline SHA-256 mismatch: expected=$expected_pipeline_sha actual=$pipeline_sha" >&2
    exit 1
}
[[ $verifier_sha == "$expected_verifier_sha" ]] || {
    echo "plugin verifier SHA-256 mismatch: expected=$expected_verifier_sha actual=$verifier_sha" >&2
    exit 1
}

sidecar_sha() {
    local epoch=$1
    local name="epoch-$epoch.jet"
    local archive="$archive_dir/$name"
    local sidecar="$archive.sha256"
    [[ -f $archive && ! -L $archive && -f $sidecar && ! -L $sidecar ]] || return 1

    local line
    line=$(<"$sidecar")
    if [[ ! $line =~ ^([0-9a-f]{64})\ \ (epoch-[0-9]+\.jet)$ ]] \
        || [[ ${BASH_REMATCH[2]:-} != "$name" ]]; then
        echo "invalid SHA-256 sidecar format: $sidecar" >&2
        return 2
    fi
    printf '%s\n' "${BASH_REMATCH[1]}"
}

while true; do
    pending=()
    for ((epoch = start_epoch; epoch <= end_epoch; epoch++)); do
        receipt="$state_dir/receipts/epoch-$epoch.plugin.ok"
        receipt_archive_sha=""
        if [[ -e $receipt && (! -f $receipt || -L $receipt) ]]; then
            echo "plugin receipt must be a regular file, not a symlink: $receipt" >&2
            exit 1
        fi
        if [[ -f $receipt ]]; then
            receipt_line=$(<"$receipt")
            if [[ $receipt_line =~ ^([0-9a-f]{64})\ ([0-9a-f]{64})\ ([0-9a-f]{64})$ ]] \
                && [[ ${BASH_REMATCH[2]} == "$pipeline_sha" ]] \
                && [[ ${BASH_REMATCH[3]} == "$verifier_sha" ]]; then
                receipt_archive_sha=${BASH_REMATCH[1]}
            fi
        fi

        if archive_sha=$(sidecar_sha "$epoch"); then
            :
        else
            status=$?
            ((status == 1)) || exit "$status"
            if [[ -n $receipt_archive_sha ]]; then
                continue
            fi
            pending+=("$epoch")
            continue
        fi

        if [[ $receipt_archive_sha == "$archive_sha" ]]; then
            continue
        fi

        echo "[$(date -u +%FT%TZ)] dispatching sealed plugin verification for epoch $epoch"
        "$verifier" "$pipeline" "$archive_dir" "$state_dir" \
            "$epoch" "$epoch" "$threads" "$poll_seconds"
    done

    if ((${#pending[@]} == 0)); then
        echo "[$(date -u +%FT%TZ)] plugin verification complete for epochs $start_epoch-$end_epoch"
        exit 0
    fi
    printf '[%s] waiting for unverified archive pairs; missing=%s\n' \
        "$(date -u +%FT%TZ)" "${pending[*]}"
    sleep "$poll_seconds"
done
