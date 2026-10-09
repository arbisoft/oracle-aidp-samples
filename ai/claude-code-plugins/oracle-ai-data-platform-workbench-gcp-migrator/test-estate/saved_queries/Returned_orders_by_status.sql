SELECT status, COUNTIF(status = 'Returned') AS returned, COUNT(*) AS total
FROM `migration_test.orders`
GROUP BY status
