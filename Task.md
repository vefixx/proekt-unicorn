# Направление 1A
## Потребление электроэнергии/воды за час
так как показания снимаются каждый час, то будем использовать функцию LAG, позволяющая получать предыдущую строку. Будем получать предыдущую строку по device_id и metric_type.code, записывать предыдущее значение в поле prev_value и вычитать из текущего (так мы получим разницу текущего и предыдущего часа)
```sql
DROP VIEW IF EXISTS view_resource_consumption_hourly;
CREATE VIEW view_resource_consumption_hourly AS
WITH base_cte AS (
	SELECT
		m.measurement_id,
		m.ts,
		b.building_id,
		c.name AS complex,
		b.name AS building,
		d.apartment_id,
		a.apartment_no,
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
	building_id,
	complex,
	building,
	apartment_id,
	apartment_no,
	unit,
	code,
	CAST(ROUND(
		CASE
			WHEN prev_value IS NULL THEN 0
			ELSE value_num - prev_value
		END,
		3
	) AS REAL) AS value
FROM base_cte;
```
## Потребление электроэнергии/воды за день
Для получения данных за день будем использовать уже существующее представление view_resource_consumption_hourly. Будем группировать строки по дате (функция DATE() возвращает год, месяц и день) и по всем остальным столбцам, и суммировать данные из каждой группы.
```sql
DROP VIEW IF EXISTS view_resource_consumption_daily;

CREATE VIEW view_resource_consumption_daily AS
SELECT
    DATE(ts) AS date,
    building_id,
    building,
    complex,
    apartment_id,
    apartment_no,
    code,
    unit,
    ROUND(SUM(value), 3) AS value
FROM view_resource_consumption_hourly
GROUP BY
    DATE(ts),
    complex,
    building_id,
    building,
    apartment_id,
    apartment_no,
    code,
    unit;
```

## Выявление опасных сценариев энергопотребления при отсутствии движения
Выявление опасных сценариев высокого потребления электроэнергии при отсутствии движения. Используя представление `view_resource_consumption_hourly`, с данными о потреблении за прошедший час,  посчитаем процент повышения/понижения потребления.
Для каждой строки найдем разницу в процентах между текущим и предыдущим значением


**SQL-запрос создания представления с процентами потребления**
```sql
DROP VIEW IF EXISTS view_energy_delta_percent_by_apartment;
CREATE VIEW view_energy_delta_percent_by_apartment AS
WITH delta_cte AS (
    SELECT
        ts,
        complex,
        building_id,
        building,
        apartment_id,
        apartment_no,
        value,
        LAG(value) OVER (PARTITION BY complex, building, apartment_id, apartment_no ORDER BY ts) AS prev_value
    FROM view_resource_consumption_hourly
    WHERE code = 'electricity_kwh_total'
)
SELECT 
    ts,
    complex,
    building_id,
    building,
    apartment_id,
    apartment_no,
    CASE 
        WHEN prev_value IS NULL OR prev_value = 0 THEN 0
        ELSE ROUND(((value - prev_value) * 100.0 / prev_value), 2)
    END AS delta_percent
FROM delta_cte
```


Теперь создадим представление с наличием движения в квартире. *В перспективе лучше сгруппировать так, что если хотя бы в одной из комнат есть движение, то пометим что во всей квартире есть движение. Но посмотрев данные об устройствах этого не требуется, так как на одну квартиру установлено одно устройство*
```sql
DROP VIEW IF EXISTS view_motion_detect_by_apartment;
CREATE VIEW view_motion_detect_by_apartment AS
WITH base_cte AS (
    SELECT
        m.ts,
        a.building_id,
        d.apartment_id,
        a.apartment_no,
        m.value_bool AS has_motion
    FROM measurement m
    JOIN device d ON d.device_id = m.device_id
    JOIN metric_type mt ON mt.metric_type_id = m.metric_type_id
    JOIN apartment a ON a.apartment_id = d.apartment_id
    WHERE mt.code = 'motion_detected'
)
SELECT
    ts,
    building_id,
    apartment_id,
    apartment_no,
    has_motion
FROM base_cte;
```

В конце создадим финальное представление. Опасным сценарием будем помечать строки, где потребление выросло на +30 или более процентов и движения в это время не было.
```sql
DROP VIEW IF EXISTS view_danger_energy_with_no_motion_scen;
CREATE VIEW view_danger_energy_with_no_motion_scen AS
SELECT 
    venergy.ts,
    venergy.building_id,
    venergy.building,
    venergy.complex,
    venergy.apartment_id,
    venergy.apartment_no,
    CAST(venergy.delta_percent AS REAL) AS delta_percent,
    vmotion.has_motion,
    CASE
        WHEN venergy.delta_percent > 30 AND vmotion.has_motion == 0 THEN 1
        ELSE 0
    END AS is_danger
FROM view_energy_delta_percent_by_apartment venergy
JOIN view_motion_detect_by_apartment vmotion ON vmotion.ts = venergy.ts AND vmotion.apartment_id = venergy.apartment_id AND vmotion.building_id = venergy.building_id
```


## Формирование журнала инцидентов
Для формирования журнала создадим физическую таблицу, в которую будет записывать все **новые** инциденты из представлений. Заполнять таблицу будем скриптом. Скрипт реализуем на языке C#. Сценарий примерно следующий:
1. Открываем базу данных
2. Получаем дату и время последнего инцидента
3. Проходимся поочередно по всем представлениям с опасными сценариями, аномалиями и т.д., получаем в них все записи, дата которых старше последнего инцидента в физ. таблице
4. Добавляем эти данные в физ. таблицу, разбивая их на категории

Ссылка на репозиторий с исходным кодом скрипта с инструкцией к нему (README): https://github.com/vefixx/UnicornDatabaseIncidentJournalScript/tree/master

Далее будем запускать этот скрипт по рассписанию. Лично я использовал свой личный VDS, добавлял запись в crontab для запуска скрипта раз в час.
*Уточнение - база данных должна находится в том же хранилище, где и скрипт (т.е. находиться на одном сервере).*

 Структура физической таблицы будет следующая:
 - incident_id (integer - primary key)
 - ts (дата и время)
 - building_id (ключ к таблице building)
 - building (string)
 - complex (string)
 - apartment_id (ключ к таблице apartment)
 - apartment_no (string)
 - category (string)
 - message (string)
