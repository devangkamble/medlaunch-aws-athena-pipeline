-- sql/stage3_state_counts.sql
SELECT
  location.state AS state,
  COUNT(1)        AS facilities
FROM healthcare_db.facility_data
GROUP BY location.state
ORDER BY facilities DESC;
