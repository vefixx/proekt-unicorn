# 1A
## view_resource_consumption_hourly
так как показания снимаются каждый час, то будем использовать функцию LAG, позволяющая получать предыдущую строку. Будем получать предыдущую строку по device_id и metric_type.code, записывать предыдущее значение в поле prev_value и вычитать из текущего (так мы получим разницу текущего и предыдущего часа)
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
## view_resource_consumption_daily
Для получения данных за день будем использовать уже существующее представление view_resource_consumption_hourly. Будем группировать строки по дате (функция DATE() возвращает год, месяц и день) и по всем остальным столбцам, и суммировать данные из каждой группы.
```sql
DROP VIEW IF EXISTS view_resource_consumption_daily;

CREATE VIEW view_resource_consumption_daily AS
SELECT
    DATE(ts) AS date,
    complex,
    building,
    apartment,
    code,
    unit,
    ROUND(SUM(value), 3) AS value
FROM view_resource_consumption_hourly
GROUP BY
    DATE(ts),
    complex,
    building,
    apartment,
    code,
    unit;
```

## Выявление опасных сценариев энергопотребления при отсутствии движения
Выявление опасных сценариев высокого потребления электроэнергии при отсутствии движения. Для каждой квартиры выстроим волновую диаграмму потребления электроэнергии с разницой текущего часа и предыдущего. Опасным сценарием будет **пик** в этой диаграмме, когда потребление резко возрасло, но движения в это время не было.

Чтобы собрать квартиры будем группировать по ее номеру, комплексу и строению. Далее, отсортируем строки по дате, и для каждой строки будем смотреть предыдущий час. Если разница текущего часа и предыдущего составляется +30% (или иной процент).

```sql
DROP VIEW IF EXISTS view_energy_delta_percent_by_apartment;
CREATE VIEW view_energy_delta_percent_by_apartment AS
WITH delta_cte AS (
    SELECT
        ts,
        complex,
        building,
        apartment,
        value,
        LAG(value) OVER (PARTITION BY complex, building, apartment ORDER BY ts) AS prev_value
    FROM view_resource_consumption_hourly
    WHERE code = 'electricity_kwh_total'
)
SELECT 
    ts,
    complex,
    building,
    apartment,
    CASE 
        WHEN prev_value IS NULL OR prev_value = 0 THEN 0
        ELSE ROUND(((value - prev_value) * 100.0 / prev_value), 2)
    END AS delta_percent
FROM delta_cte
```


Теперь построим представление, чтобы сопоставить часы и наличие движения в квартире.
