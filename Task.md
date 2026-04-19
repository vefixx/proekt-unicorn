# 1A
## view_resource_consumption_hourly
так как показания снимаются каждый час, то будем использовать функцию LAG, позволяющая получать предыдущую строку. Будем получать предыдущую строку по device_id и metric_type.code, записывать предыдущее значение в поле prev_value и вычитать из текущего (так мы получим разницу текущего и предыдущего дня)
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

## view_motion_detect_and_power_by_room
Выявление аномального потребление электроэнергии в ночной период.
Создадим представление, используя предыдущие данные (потребление за час). Составим таблицу, которая будет отображать наличие движение и потребленную электроэнергию в конкретное время.
Далее выявим порог потребления. Вычислим [перцентиль](https://habr.com/ru/companies/tochka/articles/690814/) для каждого комплекса, строения и квартиры (далее будем называть объект). Анализировать данные будем только в ночное время.
Переберем значения power_kWh от 0.1 до 0.9 (далее - n). Найдем n, при котором 95% значений < n, а оставшиеся больше. Если такой нашли, то будем называть, для данного объекта значения выше n являются аномальными 
```sql
DROP VIEW IF EXISTS view_motion_detect_and_power_by_apartment;
CREATE VIEW view_motion_detect_and_power_by_apartment AS
WITH motion_cte AS (
    SELECT
        m.ts,
        c.name AS complex,
        b.name AS building,
        a.apartment_no AS apartment,
        MAX(m.value_bool) AS motion_detect
    FROM measurement m
    JOIN metric_type mt ON mt.metric_type_id = m.metric_type_id
    JOIN device d ON d.device_id = m.device_id
    JOIN apartment a ON a.apartment_id = d.apartment_id
    JOIN building b ON b.building_id = a.building_id
    JOIN complex c ON c.complex_id = b.complex_id
    WHERE mt.code = 'motion_detected' AND CAST(strftime('%H', m.ts) AS INT) BETWEEN 1 AND 5
    GROUP BY m.ts, c.name, b.name, a.apartment_no
),
power_cte AS (
    SELECT 
        ts,
        complex,
        building,
        apartment,
        value AS power_kWh
    FROM view_resource_consumption_hourly
    WHERE code = 'electricity_kwh_total'
)
SELECT 
    mcte.ts,
    mcte.complex,
    mcte.building,
    mcte.apartment,
    mcte.motion_detect,
    pcte.power_kWh
FROM motion_cte mcte
JOIN power_cte pcte 
    ON pcte.ts = mcte.ts 
    AND pcte.complex = mcte.complex 
    AND pcte.building = mcte.building
    AND pcte.apartment = mcte.apartment;
    
```
