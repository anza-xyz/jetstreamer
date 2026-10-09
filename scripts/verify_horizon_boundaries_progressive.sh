#!/usr/bin/env bash
# Verify each available boundary between adjacent Horizon archives.
#
# A successful receipt binds both archive SHA-256 values to the exact verifier
# and this script. Each archive must already have a matching full-verification
# receipt. The boundary verifier therefore only needs to decode the left
# terminal edge and right initial edge; the independent full receipts prove
# all within-archive structure and PoH for those exact SHA-256 values.
set -Eeuo pipefail

umask 077
export LC_ALL=C
export PATH=/usr/bin:/bin

usage() {
    echo "usage: $0 VERIFY_BOUNDARY ARCHIVE_DIR FULL_RECEIPT_DIR FULL_VERIFIER_SHA256_LIST FULL_SCRIPT_SHA256 STATE_DIR START_EPOCH END_EPOCH [POLL_SECONDS]" >&2
    exit 2
}

[[ $# -ge 8 && $# -le 9 ]] || usage

verifier=$1
archive_dir=$2
full_receipt_dir=$3
full_verifier_shas=$4
full_script_sha=$5
state_dir=$6
start_epoch=$7
end_epoch=$8
poll_seconds=${9:-300}

for value in "$start_epoch" "$end_epoch" "$poll_seconds"; do
    [[ $value =~ ^[0-9]+$ ]] || usage
done
((start_epoch < end_epoch)) || usage
((poll_seconds >= 1 && poll_seconds <= 86400)) || usage
[[ $verifier == /* && $archive_dir == /* && $full_receipt_dir == /* && $state_dir == /* ]] || usage
[[ $full_verifier_shas =~ ^[0-9a-f]{64}(,[0-9a-f]{64})*$ \
    && $full_script_sha =~ ^[0-9a-f]{64}$ ]] || usage
[[ -f $verifier && -x $verifier && ! -L $verifier ]] || {
    echo "verifier must be an executable regular file, not a symlink: $verifier" >&2
    exit 2
}
[[ -d $archive_dir && ! -L $archive_dir ]] || {
    echo "archive directory must be a regular directory, not a symlink: $archive_dir" >&2
    exit 2
}
[[ -d $full_receipt_dir && ! -L $full_receipt_dir ]] || {
    echo "full receipt directory must be a regular directory, not a symlink: $full_receipt_dir" >&2
    exit 2
}

verifier=$(realpath "$verifier")
archive_dir=$(realpath "$archive_dir")
full_receipt_dir=$(realpath "$full_receipt_dir")
mkdir -p "$state_dir/logs" "$state_dir/receipts" "$state_dir/tmp"
for directory in "$state_dir" "$state_dir/logs" "$state_dir/receipts" "$state_dir/tmp"; do
    [[ -d $directory && ! -L $directory ]] || {
        echo "boundary verification state paths must be regular directories: $directory" >&2
        exit 2
    }
done
state_dir=$(realpath "$state_dir")

fsync_exact() {
    local path=$1
    /usr/bin/python3 - "$path" <<'PY'
import os
import stat
import sys

path = sys.argv[1]
before = os.lstat(path)
if stat.S_ISLNK(before.st_mode):
    raise SystemExit(f"refusing to fsync symlink: {path}")
flags = os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW
if stat.S_ISDIR(before.st_mode):
    flags |= os.O_DIRECTORY
elif not stat.S_ISREG(before.st_mode):
    raise SystemExit(f"fsync target is not a regular file or directory: {path}")
descriptor = os.open(path, flags)
try:
    after = os.fstat(descriptor)
    if (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino):
        raise SystemExit(f"fsync target changed while opening: {path}")
    os.fsync(descriptor)
finally:
    os.close(descriptor)
PY
}

verifier_sha=$(sha256sum --binary "$verifier" | cut -d' ' -f1)
script_sha=$(sha256sum --binary "$0" | cut -d' ' -f1)

validate_pair() {
    local epoch=$1
    local name="epoch-$epoch.jet"
    local archive="$archive_dir/$name"
    local sidecar="$archive.sha256"
    [[ -f $archive && ! -L $archive && -f $sidecar && ! -L $sidecar ]] || return 1

    local line expected
    line=$(<"$sidecar")
    if [[ ! $line =~ ^([0-9a-f]{64})\ \ (epoch-[0-9]+\.jet)$ ]] \
        || [[ ${BASH_REMATCH[2]:-} != "$name" ]]; then
        echo "invalid SHA-256 sidecar format: $sidecar" >&2
        return 2
    fi
    expected=${BASH_REMATCH[1]}

    local full_receipt="$full_receipt_dir/epoch-$epoch.full.ok"
    local full_sha="" full_verifier="" full_script="" extra=""
    [[ -f $full_receipt && ! -L $full_receipt ]] || return 1
    read -r full_sha full_verifier full_script extra <"$full_receipt" || return 1
    [[ $full_sha =~ ^[0-9a-f]{64}$ \
        && $full_verifier =~ ^[0-9a-f]{64}$ \
        && $full_script =~ ^[0-9a-f]{64}$ \
        && -z $extra ]] || {
        echo "invalid full-verification receipt: $full_receipt" >&2
        return 2
    }
    case ",$full_verifier_shas," in
        *",$full_verifier,"*) ;;
        *)
            echo "full-verification receipt uses an unapproved verifier for $sidecar" >&2
            return 2
            ;;
    esac
    [[ $full_sha == "$expected" && $full_script == "$full_script_sha" ]] || {
        echo "full-verification receipt does not match approved evidence for $sidecar" >&2
        return 2
    }
    printf '%s\n' "$expected"
}

archive_identity() {
    local epoch=$1
    local archive="$archive_dir/epoch-$epoch.jet"
    [[ -f $archive && ! -L $archive ]] || return 1
    stat --format='%d:%i:%s:%y:%z:%h' -- "$archive"
}

receipt_is_current() {
    local receipt=$1
    local expected_left=${2:-}
    local expected_right=${3:-}
    local left_sha="" right_sha="" got_verifier="" got_script="" extra=""
    [[ -f $receipt && ! -L $receipt ]] || return 1
    read -r left_sha right_sha got_verifier got_script extra <"$receipt" || return 1
    [[ $left_sha =~ ^[0-9a-f]{64}$ \
        && $right_sha =~ ^[0-9a-f]{64}$ \
        && $got_verifier == "$verifier_sha" \
        && $got_script == "$script_sha" \
        && -z $extra \
        && (-z $expected_left || $left_sha == "$expected_left") \
        && (-z $expected_right || $right_sha == "$expected_right") ]]
}

while true; do
    pending=()
    progressed=0
    for ((left = start_epoch; left < end_epoch; left++)); do
        right=$((left + 1))
        receipt="$state_dir/receipts/boundary-$left-$right.ok"

        # A prior fsynced receipt remains authoritative after safe local
        # retirement. Its archive hashes are cross-checked again whenever the
        # local pair is still available.
        if receipt_is_current "$receipt" && \
            [[ ! -e $archive_dir/epoch-$left.jet || ! -e $archive_dir/epoch-$right.jet ]]; then
            continue
        fi

        if left_sha=$(validate_pair "$left"); then
            :
        else
            status=$?
            ((status == 1)) || exit "$status"
            pending+=("$left-$right")
            continue
        fi
        if right_sha=$(validate_pair "$right"); then
            :
        else
            status=$?
            ((status == 1)) || exit "$status"
            pending+=("$left-$right")
            continue
        fi
        if receipt_is_current "$receipt" "$left_sha" "$right_sha"; then
            continue
        fi

        left_archive="$archive_dir/epoch-$left.jet"
        right_archive="$archive_dir/epoch-$right.jet"
        left_identity=$(archive_identity "$left") || exit $?
        right_identity=$(archive_identity "$right") || exit $?
        log_tmp="$state_dir/tmp/boundary-$left-$right.$$.tmp"
        echo "[$(date -u +%FT%TZ)] boundary $left-$right verification started"
        if nice -n 19 ionice -c 3 \
            "$verifier" --verify-pair "$left_archive" "$right_archive" \
            >"$log_tmp" 2>&1; then
            after_left=$(validate_pair "$left") || exit $?
            after_right=$(validate_pair "$right") || exit $?
            [[ $after_left == "$left_sha" && $after_right == "$right_sha" ]] || {
                echo "archive evidence changed during boundary $left-$right verification" >&2
                exit 1
            }
            [[ $(archive_identity "$left") == "$left_identity" \
                && $(archive_identity "$right") == "$right_identity" ]] || {
                echo "archive identity changed during boundary $left-$right verification" >&2
                exit 1
            }
            [[ $(sha256sum --binary "$verifier" | cut -d' ' -f1) == "$verifier_sha" ]] || {
                echo "verifier changed during boundary $left-$right verification" >&2
                exit 1
            }

            mv -f -- "$log_tmp" "$state_dir/logs/boundary-$left-$right.log"
            receipt_tmp="$state_dir/tmp/boundary-$left-$right-receipt.$$.tmp"
            printf '%s %s %s %s\n' \
                "$left_sha" "$right_sha" "$verifier_sha" "$script_sha" >"$receipt_tmp"
            chmod 600 "$receipt_tmp"
            fsync_exact "$receipt_tmp"
            mv -f -- "$receipt_tmp" "$receipt"
            fsync_exact "$state_dir/receipts"
            echo "[$(date -u +%FT%TZ)] boundary $left-$right verification passed; receipt=$receipt"
            progressed=1
            continue
        fi

        mv -f -- "$log_tmp" \
            "$state_dir/logs/boundary-$left-$right.failed.$(date -u +%Y%m%dT%H%M%SZ).log"
        echo "boundary $left-$right verification failed" >&2
        exit 1
    done

    if ((${#pending[@]} == 0)); then
        echo "[$(date -u +%FT%TZ)] boundary verification complete for epochs $start_epoch-$end_epoch"
        exit 0
    fi
    printf '[%s] waiting for adjacent archive pairs; missing=%s\n' \
        "$(date -u +%FT%TZ)" "${pending[*]}"
    sleep "$poll_seconds"
done
