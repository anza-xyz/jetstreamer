#!/usr/bin/env bash
set -euo pipefail

# Calculates the total size (in bytes) of all Old Faithful epoch CAR files
# plus their compact indexes (slot-to-cid and cid-to-offset-and-size).
# Uses HEAD requests from epoch 0 onward until a 404 is encountered.

BASE_URL="${JETSTREAMER_HTTP_BASE_URL:-${JETSTREAMER_ARCHIVE_BASE:-https://files.old-faithful.net}}"
BASE_URL="${BASE_URL%/}"
INDEX_BASE_URL="${JETSTREAMER_COMPACT_INDEX_BASE_URL:-${JETSTREAMER_ARCHIVE_BASE:-$BASE_URL}}"
INDEX_BASE_URL="${INDEX_BASE_URL%/}"
NETWORK="${JETSTREAMER_NETWORK:-mainnet}"
START_EPOCH="${START_EPOCH:-0}"
CONCURRENCY="${CONCURRENCY:-10}"

total_bytes=0
epoch="$START_EPOCH"

printf "Base URL: %s\n" "$BASE_URL"
printf "Index base URL: %s\n" "$INDEX_BASE_URL"
printf "Network: %s\n" "$NETWORK"
printf "Starting at epoch: %s\n" "$epoch"

format_bytes() {
  awk -v b="$1" 'BEGIN{
    split("B KiB MiB GiB TiB PiB", u);
    i=1;
    while (b>=1024 && i<6) { b/=1024; i++; }
    printf "%.2f %s", b, u[i];
  }'
}

get_root_base32() {
  local url="$1"
  python3 - "$url" <<'PY'
import base64
import sys
import urllib.request

url = sys.argv[1]

def fetch_range(start, end):
    req = urllib.request.Request(
        url,
        headers={
            "Range": f"bytes={start}-{end}",
            "User-Agent": "curl/8.6.0",
        },
    )
    with urllib.request.urlopen(req) as resp:
        return resp.read()

def read_varint(data, offset=0):
    value = 0
    shift = 0
    idx = offset
    while idx < len(data):
        byte = data[idx]
        idx += 1
        value |= (byte & 0x7f) << shift
        if byte < 0x80:
            return value, idx - offset
        shift += 7
        if shift > 63:
            raise ValueError("varint overflow")
    raise ValueError("buffer ended before varint terminated")

def read_uvarint(data, offset=0):
    value = 0
    shift = 0
    idx = offset
    while idx < len(data):
        byte = data[idx]
        idx += 1
        value |= (byte & 0x7f) << shift
        if byte < 0x80:
            return value, idx
        shift += 7
        if shift > 63:
            raise ValueError("varint overflow")
    raise ValueError("buffer ended before varint terminated")

def encode_uvarint(value):
    out = bytearray()
    while True:
        byte = value & 0x7f
        value >>= 7
        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            break
    return bytes(out)

def read_ai(data, offset, ai):
    if ai < 24:
        return ai, offset
    if ai == 24:
        return data[offset], offset + 1
    if ai == 25:
        return int.from_bytes(data[offset:offset+2], "big"), offset + 2
    if ai == 26:
        return int.from_bytes(data[offset:offset+4], "big"), offset + 4
    if ai == 27:
        return int.from_bytes(data[offset:offset+8], "big"), offset + 8
    raise ValueError("indefinite length not supported")

def decode_cbor(data, offset=0):
    if offset >= len(data):
        raise ValueError("unexpected end of CBOR data")
    initial = data[offset]
    offset += 1
    major = initial >> 5
    ai = initial & 0x1f

    if major == 0:
        val, offset = read_ai(data, offset, ai)
        return val, offset
    if major == 1:
        val, offset = read_ai(data, offset, ai)
        return -1 - val, offset
    if major == 2:
        length, offset = read_ai(data, offset, ai)
        return data[offset:offset+length], offset + length
    if major == 3:
        length, offset = read_ai(data, offset, ai)
        return data[offset:offset+length].decode("utf-8"), offset + length
    if major == 4:
        length, offset = read_ai(data, offset, ai)
        arr = []
        for _ in range(length):
            item, offset = decode_cbor(data, offset)
            arr.append(item)
        return arr, offset
    if major == 5:
        length, offset = read_ai(data, offset, ai)
        mp = {}
        for _ in range(length):
            key, offset = decode_cbor(data, offset)
            val, offset = decode_cbor(data, offset)
            mp[key] = val
        return mp, offset
    if major == 6:
        tag, offset = read_ai(data, offset, ai)
        item, offset = decode_cbor(data, offset)
        return ("tag", tag, item), offset
    if major == 7:
        if ai == 20:
            return False, offset
        if ai == 21:
            return True, offset
        if ai == 22:
            return None, offset
        if ai == 23:
            return None, offset
        raise ValueError("unsupported simple/float type")
    raise ValueError("unsupported CBOR major type")

data = fetch_range(0, 4095)
header_len, prefix = read_varint(data, 0)
total_needed = prefix + header_len
if len(data) < total_needed:
    data = fetch_range(0, total_needed - 1)
    if len(data) < total_needed:
        raise ValueError("incomplete CAR header")

header_bytes = data[prefix:total_needed]
header, _ = decode_cbor(header_bytes, 0)
if not isinstance(header, dict):
    raise ValueError("CAR header is not a map")
roots = header.get("roots")
if not isinstance(roots, list) or not roots:
    raise ValueError("CAR header missing roots")
root = roots[0]
if isinstance(root, tuple) and len(root) == 3 and root[0] == "tag" and root[1] == 42:
    root = root[2]
if not isinstance(root, (bytes, bytearray)):
    raise ValueError("unexpected CID encoding in CAR header")

cid_bytes = bytes(root)
candidates = []
if cid_bytes and cid_bytes[0] == 0 and len(cid_bytes) > 1:
    candidates.append(cid_bytes[1:])
candidates.append(cid_bytes)

last_err = None
for candidate in candidates:
    try:
        version, idx = read_uvarint(candidate, 0)
        if version == 1:
            cid_v1 = candidate
        else:
            cid_v1 = encode_uvarint(1) + encode_uvarint(0x70) + candidate
        b32 = base64.b32encode(cid_v1).decode("ascii").lower().rstrip("=")
        print("b" + b32)
        sys.exit(0)
    except Exception as exc:
        last_err = exc

raise SystemExit(str(last_err) if last_err else "failed to decode CID")
PY
}

fetch_epoch_info() {
  local epoch="$1"
  local url="${BASE_URL}/${epoch}/epoch-${epoch}.car"

  local response http_code content_length
  if ! response=$(curl -sS -I -L -w "\n%{http_code}\n" "$url"); then
    printf "curl failed for epoch %s (%s)\n" "$epoch" "$url" >&2
    return 1
  fi

  http_code=$(printf "%s" "$response" | tail -n 1 | tr -d '\r')

  if [[ "$http_code" == "404" ]]; then
    printf "notfound\t%s\n" "$epoch"
    return 2
  fi

  if [[ "$http_code" != "200" && "$http_code" != "206" ]]; then
    printf "Unexpected HTTP %s for epoch %s (%s). Aborting.\n" "$http_code" "$epoch" "$url" >&2
    return 1
  fi

  content_length=$(
    printf "%s" "$response" \
      | tr -d '\r' \
      | awk 'tolower($1)=="content-length:" {val=$2} END{print val}'
  )

  if [[ -z "$content_length" ]]; then
    printf "Missing Content-Length for epoch %s (%s). Aborting.\n" "$epoch" "$url" >&2
    return 1
  fi

  if ! [[ "$content_length" =~ ^[0-9]+$ ]]; then
    printf "Non-numeric Content-Length (%s) for epoch %s. Aborting.\n" "$content_length" "$epoch" >&2
    return 1
  fi

  local root_base32
  if ! root_base32=$(get_root_base32 "$url"); then
    printf "Failed to read CAR header for epoch %s (%s). Aborting.\n" "$epoch" "$url" >&2
    return 1
  fi

  local slot_index_url="${INDEX_BASE_URL}/${epoch}/epoch-${epoch}-${root_base32}-${NETWORK}-slot-to-cid.index"
  local cid_index_url="${INDEX_BASE_URL}/${epoch}/epoch-${epoch}-${root_base32}-${NETWORK}-cid-to-offset-and-size.index"

  local slot_size=0
  local slot_missing=0
  if response=$(curl -sS -I -L -w "\n%{http_code}\n" "$slot_index_url"); then
    local slot_code
    slot_code=$(printf "%s" "$response" | tail -n 1 | tr -d '\r')
    if [[ "$slot_code" == "200" || "$slot_code" == "206" ]]; then
      slot_size=$(printf "%s" "$response" | tr -d '\r' | awk 'tolower($1)=="content-length:" {val=$2} END{print val}')
      if [[ -z "$slot_size" || ! "$slot_size" =~ ^[0-9]+$ ]]; then
        printf "Invalid Content-Length for slot index epoch %s (%s). Aborting.\n" "$epoch" "$slot_index_url" >&2
        return 1
      fi
    elif [[ "$slot_code" == "404" ]]; then
      slot_missing=1
      slot_size=0
    else
      printf "Unexpected HTTP %s for slot index epoch %s (%s). Aborting.\n" "$slot_code" "$epoch" "$slot_index_url" >&2
      return 1
    fi
  else
    printf "curl failed for slot index epoch %s (%s)\n" "$epoch" "$slot_index_url" >&2
    return 1
  fi

  local cid_size=0
  local cid_missing=0
  if response=$(curl -sS -I -L -w "\n%{http_code}\n" "$cid_index_url"); then
    local cid_code
    cid_code=$(printf "%s" "$response" | tail -n 1 | tr -d '\r')
    if [[ "$cid_code" == "200" || "$cid_code" == "206" ]]; then
      cid_size=$(printf "%s" "$response" | tr -d '\r' | awk 'tolower($1)=="content-length:" {val=$2} END{print val}')
      if [[ -z "$cid_size" || ! "$cid_size" =~ ^[0-9]+$ ]]; then
        printf "Invalid Content-Length for cid index epoch %s (%s). Aborting.\n" "$epoch" "$cid_index_url" >&2
        return 1
      fi
    elif [[ "$cid_code" == "404" ]]; then
      cid_missing=1
      cid_size=0
    else
      printf "Unexpected HTTP %s for cid index epoch %s (%s). Aborting.\n" "$cid_code" "$epoch" "$cid_index_url" >&2
      return 1
    fi
  else
    printf "curl failed for cid index epoch %s (%s)\n" "$epoch" "$cid_index_url" >&2
    return 1
  fi

  printf "ok\t%s\t%s\t%s\t%s\t%s\t%s\n" \
    "$epoch" "$content_length" "$slot_size" "$cid_size" "$slot_missing" "$cid_missing"
  return 0
}

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

missing_epoch=""

spawn_epoch() {
  local epoch="$1"
  local outfile="${tmp_dir}/epoch-${epoch}.out"
  (fetch_epoch_info "$epoch" >"$outfile") &
  pids+=("$!")
  epochs+=("$epoch")
  files+=("$outfile")
}

while :; do
  pids=()
  epochs=()
  files=()
  while [[ -z "$missing_epoch" && ${#pids[@]} -lt "$CONCURRENCY" ]]; do
    spawn_epoch "$epoch"
    epoch=$((epoch + 1))
  done

  if [[ ${#pids[@]} -eq 0 ]]; then
    break
  fi

  for i in "${!pids[@]}"; do
    pid="${pids[i]}"
    epoch_i="${epochs[i]}"
    outfile="${files[i]}"

    if wait "$pid"; then
      status=0
    else
      status=$?
    fi

    line=""
    if [[ -f "$outfile" ]]; then
      line=$(cat "$outfile")
    fi

    if [[ "$status" -eq 2 ]]; then
      if [[ -z "$missing_epoch" ]]; then
        missing_epoch="$epoch_i"
        printf "Epoch %s not found (HTTP 404). Stopping after in-flight epochs complete.\n" "$epoch_i"
      fi
    elif [[ "$status" -ne 0 ]]; then
      printf "Failed while fetching epoch %s. See errors above.\n" "$epoch_i" >&2
      exit 1
    else
      IFS=$'\t' read -r tag epoch_val car_size slot_size cid_size slot_missing cid_missing <<<"$line"
      if [[ "$tag" != "ok" ]]; then
        printf "Unexpected output while fetching epoch %s. Aborting.\n" "$epoch_i" >&2
        exit 1
      fi

      if [[ -n "$missing_epoch" && "$epoch_val" -ge "$missing_epoch" ]]; then
        printf "Epoch %s skipped (after 404 at epoch %s).\n" "$epoch_val" "$missing_epoch"
      else
        epoch_total=$((car_size + slot_size + cid_size))
        total_bytes=$((total_bytes + epoch_total))
        car_human=$(format_bytes "$car_size")
        slot_human=$(format_bytes "$slot_size")
        cid_human=$(format_bytes "$cid_size")
        epoch_human=$(format_bytes "$epoch_total")
        total_human=$(format_bytes "$total_bytes")
        slot_suffix=""
        cid_suffix=""
        if [[ "$slot_missing" == "1" ]]; then slot_suffix=" (missing)"; fi
        if [[ "$cid_missing" == "1" ]]; then cid_suffix=" (missing)"; fi
        printf "Epoch %s: car=%s (%s), slot-index=%s (%s)%s, cid-index=%s (%s)%s, epoch total=%s (%s), running total=%s (%s)\n" \
          "$epoch_val" "$car_size" "$car_human" \
          "$slot_size" "$slot_human" "$slot_suffix" \
          "$cid_size" "$cid_human" "$cid_suffix" \
          "$epoch_total" "$epoch_human" \
          "$total_bytes" "$total_human"
      fi
    fi

    rm -f "$outfile"
  done

  if [[ -n "$missing_epoch" ]]; then
    break
  fi
done

final_human=$(format_bytes "$total_bytes")
if [[ -n "$missing_epoch" ]]; then
  end_epoch=$((missing_epoch - 1))
else
  end_epoch=$((epoch - 1))
fi
printf "Final total: %s bytes (%s) (epochs %s..%s)\n" \
  "$total_bytes" "$final_human" "$START_EPOCH" "$end_epoch"
