#!/usr/bin/env bash
# Stream each available Horizon archive through the current read-only plugin
# pipeline and bind a durable per-epoch receipt to the exact archive bytes.
set -Eeuo pipefail

umask 077
export LC_ALL=C
export PATH=/usr/bin:/bin

usage() {
    echo "usage: $0 HORIZON_PIPELINE ARCHIVE_DIR STATE_DIR START_EPOCH END_EPOCH [THREADS] [POLL_SECONDS]" >&2
    exit 2
}

[[ $# -ge 5 && $# -le 7 ]] || usage

pipeline=$1
archive_dir=$2
state_dir=$3
start_epoch=$4
end_epoch=$5
threads=${6:-16}
poll_seconds=${7:-300}

for value in "$start_epoch" "$end_epoch" "$threads" "$poll_seconds"; do
    [[ $value =~ ^[0-9]+$ ]] || usage
done
((start_epoch <= end_epoch)) || usage
((threads >= 1 && threads <= 256)) || usage
((poll_seconds >= 1 && poll_seconds <= 86400)) || usage
[[ $pipeline == /* && $archive_dir == /* && $state_dir == /* ]] || usage
[[ -f $pipeline && -x $pipeline && ! -L $pipeline ]] || {
    echo "horizon pipeline must be an executable regular file, not a symlink: $pipeline" >&2
    exit 2
}
[[ -d $archive_dir && ! -L $archive_dir ]] || {
    echo "archive directory must be a regular directory, not a symlink: $archive_dir" >&2
    exit 2
}

pipeline=$(realpath "$pipeline")
archive_dir=$(realpath "$archive_dir")
mkdir -p "$state_dir/receipts" "$state_dir/tmp"
[[ -d $state_dir && ! -L $state_dir && -d $state_dir/receipts && ! -L $state_dir/receipts \
    && -d $state_dir/tmp && ! -L $state_dir/tmp ]] || {
    echo "plugin verification state paths must be regular directories" >&2
    exit 2
}
state_dir=$(realpath "$state_dir")

pipeline_sha=$(/usr/bin/sha256sum --binary "$pipeline" | cut -d' ' -f1)
script_sha=$(/usr/bin/sha256sum --binary "$0" | cut -d' ' -f1)

validate_pair() {
    local epoch=$1
    local name="epoch-$epoch.jet"
    local archive="$archive_dir/$name"
    local sidecar="$archive.sha256"
    [[ -f $archive && ! -L $archive && -f $sidecar && ! -L $sidecar ]] || return 1

    local line expected actual
    line=$(<"$sidecar")
    if [[ ! $line =~ ^([0-9a-f]{64})\ \ (epoch-[0-9]+\.jet)$ ]] \
        || [[ ${BASH_REMATCH[2]:-} != "$name" ]]; then
        echo "invalid SHA-256 sidecar format: $sidecar" >&2
        return 2
    fi
    expected=${BASH_REMATCH[1]}
    actual=$(/usr/bin/sha256sum --binary "$archive" | cut -d' ' -f1)
    [[ $actual == "$expected" ]] || {
        echo "SHA-256 mismatch for $archive: sidecar=$expected actual=$actual" >&2
        return 2
    }
    printf '%s\n' "$actual"
}

while true; do
    pending=()
    progressed=0
    for ((epoch = start_epoch; epoch <= end_epoch; epoch++)); do
        archive="$archive_dir/epoch-$epoch.jet"
        receipt="$state_dir/receipts/epoch-$epoch.plugin.ok"
        if archive_sha=$(validate_pair "$epoch"); then
            :
        else
            status=$?
            ((status == 1)) || exit "$status"
            pending+=("$epoch")
            continue
        fi

        expected_receipt="$archive_sha $pipeline_sha $script_sha"
        if [[ -f $receipt && ! -L $receipt ]] && [[ $(<"$receipt") == "$expected_receipt" ]]; then
            continue
        fi

        echo "[$(date -u +%FT%TZ)] epoch $epoch plugin verification started"
        nice -n 19 ionice -c 3 \
            "$pipeline" "$epoch:$epoch" "$archive_dir" \
            --threads "$threads" --verify-only

        after_sha=$(validate_pair "$epoch") || exit $?
        [[ $after_sha == "$archive_sha" ]] || {
            echo "archive changed during plugin verification: $archive" >&2
            exit 1
        }
        [[ $(/usr/bin/sha256sum --binary "$pipeline" | cut -d' ' -f1) == "$pipeline_sha" ]] || {
            echo "horizon pipeline changed during plugin verification: $pipeline" >&2
            exit 1
        }

        receipt_tmp="$state_dir/tmp/epoch-$epoch-receipt.$$.tmp"
        printf '%s\n' "$expected_receipt" >"$receipt_tmp"
        chmod 600 "$receipt_tmp"
        sync -f "$receipt_tmp"
        mv -f -- "$receipt_tmp" "$receipt"
        sync -f "$state_dir/receipts"
        echo "[$(date -u +%FT%TZ)] epoch $epoch plugin verification passed; receipt=$receipt"
        progressed=1
    done

    if ((${#pending[@]} == 0)); then
        echo "[$(date -u +%FT%TZ)] plugin verification complete for epochs $start_epoch-$end_epoch"
        exit 0
    fi
    printf '[%s] waiting for archive pairs; missing=%s\n' \
        "$(date -u +%FT%TZ)" "${pending[*]}"
    ((progressed == 0)) || sync -f "$state_dir/receipts"
    sleep "$poll_seconds"
done
