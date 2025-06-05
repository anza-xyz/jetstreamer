SELECT
    base58Encode(program_id)    AS program_id_b58,
    sum(count)                  AS total_count,
    sum(error_count)            AS total_errors,
    min(min_cus)                AS min_cus,
    max(max_cus)                AS max_cus,
    sum(total_cus)              AS total_cus,
    (total_cus / sum(count))    AS avg_cus
FROM program_invocations
GROUP BY program_id
ORDER BY total_errors DESC
LIMIT 25;
