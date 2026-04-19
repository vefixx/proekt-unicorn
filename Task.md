# 1A
## view_resource_consumption_daily
так как показания снимаются каждый час, то будем использовать функцию LAG, позволяющая получать предыдущую строку. Будем получать предыдущую строку по device_id и metric_type.code, записывать в поле prev_value и вычитать из текущего значения
```sql

DROP VIEW IF EXISTS view_resource_consumption_hourly;
CREATE VIEW view_resource_consumption_hourly AS
WITH base_cte AS (
	SELECT
		m.measurement_id,
		m.ts,
		c.name AS complex,
		b.name AS building,
		a.apartment_no AS apartment,
		mt.code,
		mt.unit,
		m.value_num,
		LAG(m.value_num) OVER (PARTITION BY d.device_id, mt.code ORDER BY m.ts) AS prev_value
	FROM measurement m
	JOIN device d ON d.device_id = m.device_id
	JOIN building b ON b.building_id = d.building_id
	JOIN complex c ON c.complex_id = b.complex_id
	JOIN apartment a ON a.apartment_id = d.apartment_id
	JOIN metric_type mt ON mt.metric_type_id = m.metric_type_id
	WHERE mt.code IN ('water_cold_m3_total', 'water_hot_m3_total', 'electricity_kwh_total')
)
SELECT
	measurement_id,
	ts,
	complex,
	building,
	apartment,
	unit,
	code,
	ROUND(
		CASE
			WHEN prev_value IS NULL THEN 0
			ELSE value_num - prev_value
		END,
		3
	) AS value
FROM base_cte;
```
