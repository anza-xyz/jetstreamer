#!/bin/sh
while true; do
  cargo run --release --bin build-index && { echo "$(date): success — sleeping 1 h"; sleep 3600; } \
               || { echo "$(date): failure — sleeping 5 s"; sleep 5; }
done

