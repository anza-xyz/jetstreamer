#!/usr/bin/env bash
# Wait for a contiguous Horizon range, then stream it through the read-only
# verification plugin. Archive integrity and PoH acceptance remain the job of
# verify_horizon_range.sh; this is the independent consumer/API acceptance gate.
set -Eeuo pipefail

umask 027
export LC_ALL=C
export PATH=/usr/bin:/bin

usage() {
    echo "usage: $0 HORIZON_PIPELINE ARCHIVE_DIR START_EPOCH END_EPOCH [THREADS] [POLL_SECONDS]" >&2
    exit 2
}

[[ $# -ge 4 && $# -le 6 ]] || usage

pipeline=$1
archive_dir=$2
start_epoch=$3
end_epoch=$4
threads=${5:-16}
poll_seconds=${6:-300}

for value in "$start_epoch" "$end_epoch" "$threads" "$poll_seconds"; do
    [[ $value =~ ^[0-9]+$ ]] || usage
done
((start_epoch <= end_epoch)) || usage
((threads >= 1 && threads <= 256)) || usage
((poll_seconds >= 1 && poll_seconds <= 86400)) || usage
[[ $pipeline == /* && $archive_dir == /* ]] || usage
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

while true; do
    missing=()
    for ((epoch = start_epoch; epoch <= end_epoch; epoch++)); do
        name="epoch-$epoch.jet"
        archive="$archive_dir/$name"
        sidecar="$archive.sha256"
        if [[ ! -f $archive || -L $archive || ! -f $sidecar || -L $sidecar ]]; then
            missing+=("$epoch")
            continue
        fi

        line=$(<"$sidecar")
        if [[ ! $line =~ ^([0-9a-f]{64})\ \ (epoch-[0-9]+\.jet)$ ]] \
            || [[ ${BASH_REMATCH[2]:-} != "$name" ]]; then
            echo "invalid SHA-256 sidecar format: $sidecar" >&2
            exit 1
        fi
    done

    if ((${#missing[@]} == 0)); then
        break
    fi
    printf '[%s] waiting for archive pairs; missing=%s\n' \
        "$(date -u +%FT%TZ)" "${missing[*]}"
    sleep "$poll_seconds"
done

echo "[$(date -u +%FT%TZ)] all archive pairs present; starting Horizon verification plugin"
exec nice -n 19 ionice -c 3 \
    "$pipeline" "$start_epoch:$end_epoch" "$archive_dir" \
    --threads "$threads" --verify-only
