SELECT
    toYYYYMMDD(date) AS date_id,
    date,
    formatDateTime(date, '%A') AS day_of_week,
    formatDateTime(date, '%B') AS month,
    toQuarter(date) AS quarter,
    NOT toDayOfWeek(date) IN (6, 7) AS is_weekday,
    toWeek(date) AS week_number
FROM (
    SELECT
        addDays(toDate('2025-01-01'), number) AS date
    FROM system.numbers
    LIMIT 365
)
