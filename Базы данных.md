# Базы данных
## Реляционные
### PostgreSQL
- Базируется на языке SQL, имеет возможность работать с данными в формате JSON.
- Поддерживает наследование - одна таблица может взаимствовать столбцы из другой.
- Поддерживает написание скриптов с помощью встроенного процедурного языка PL/pgSQL, скриптовыми языками: PL/Lua, PL/LOLCODE, PL/Perl, PL/PHP, PL/Python, PL/Ruby, PL/sh, PL/Tcl, PL/Scheme, PL/v8, или классических языков: C, C++, Java.
- Создание триггеров - например, операция INSERT может запускать триггер, проверяющая добавленную запись на соответствие определенным условиям.
- Имеет поддержка индексов следующих типов: B-дерево, хеш, GiST, GIN, BRIN, Bloom. При необходимости можно создавать новые типы индексов.
- Поддерживает одновременную модификацию БД несколькими пользователями с помощью механизма Multiversion Concurrency Control (MVCC)
- С помощью команды ROLLBACK можно отменить текущее изменение.
### MSSSQL (Microsoft SQL Server)
- MS SQL Server - система управления реляционными базами данных, работающая по клиент-серверной модели.
- Язык запросов: Transact-SQL - похож на язык программирования, так как имеет условные конструкции `if else`
- Можно создавать процедуры (функции), пример процедуры ([источник](https://skillbox.ru/media/code/baza-dannykh-ms-sql-server-chto-eto-zachem-nuzhna-kak-poyavilas-i-chem-khorosha/)):
```sql
CREATE PROCEDURE AddToCart
  @UserID int,
  @ProductID int,
  @Quantity int
AS
BEGIN
  -- Добавляем новый товар в корзину или обновляем количество, если он уже есть
  MERGE INTO Cart AS C
  USING (SELECT @UserID AS UserID, @ProductID AS ProductID) AS P
  ON C.UserID = P.UserID AND C.ProductID = P.ProductID
  WHEN MATCHED THEN UPDATE SET C.Quantity = C.Quantity + @Quantity
  WHEN NOT MATCHED THEN INSERT (UserID, ProductID, Quantity) VALUES (P.UserID, P.ProductID, @Quantity);
END;
```

Вызов процедуры:
```sql
EXEC AddToCart @UserID = 101, @ProductID = 1, @Quantity = 2;
```

#### Как работает SQL Server
<img width="1540" height="866" alt="image" src="https://github.com/user-attachments/assets/b7f06330-f77e-462a-ab35-42f27c8c0cee" />

### Maria DB
MariaDB - реляционная система управления базами данных (СУБД) с открытым исходным кодом. Создана как форк MySQL после покупки последнего компанией Oracle. Основная цель - сохранить совместимость с MySQL и при этом развивать проект как полностью открытый и независимый. 

- Поддержка невидимых столцов, которых не будет видно при запросе к таблице, если те не были запрошены
- Простая работа с JSON, позволяя манипуляровать данными, пример ([источник](https://habr.com/ru/articles/855802/)):
```sql
 SELECT JSON_UNQUOTE(JSON_EXTRACT('{"name": "Maria"}', '$.name')) as name;
 +-------+
 | name  |
 +-------+
 | Maria |
 +-------+
 ```

- Имеет совместимость с PostgreSQL, SQL Server. Можно переносить приложения не изменяя его код.
**Попытка создания таблицы без переноса**:
```sql
CREATE TABLE "CUSTOMERS"(  -- double quotes
  "CUST_ID" NUMBER(8,0),  -- double quotes
  "CUST_NAME" VARCHAR2(50));  -- Oracle’s VARCHAR2 type

ERROR 1064 (42000): You have an error in your SQL syntax; check the manual that corresponds to your MariaDB server version for the right syntax to use near '"CUSTOMERS"(
  "CUST_ID" NUMBER(8,0),
  "CUST_NAME" VARCHAR2(50))' at line 1
```

**Попытка создания таблицы после переноса**
```sql
SET SESSION sql_mode="Oracle";

CREATE TABLE "CUSTOMERS"(  -- double quotes
  "CUST_ID" NUMBER(8,0),  -- double quotes
  "CUST_NAME" VARCHAR2(50));  -- Oracle’s VARCHAR2 type

Query OK, 0 rows affected (0.365 sec)  -- it works now!
```

[источник SQL запросов](https://habr.com/ru/articles/855802/)

## NoSQL
### MongoDB
MongoDB - документоориентированная система управления базами данных (СУБД), не требующая описания схемы таблиц. Считается одним из классических примеров NoSQL-систем.

Все данные хранятся в формате `"ключ": "значение"`.

Если проводить аналогии с реляционной базой, коллекции при таком способе хранения соответствуют таблицам, а документы - строкам.
Информация отформатирована в BSON - двоичной кодировке JSON-подобных документов. Это позволяет поддерживать данные типа Date и двоичных файлов, что невозможно в JSON.
Пример документа ([источник](https://skillbox.ru/media/code/mongodb-chto-eto-za-subd-plyusy-minusy-podvodnye-kamni/)):
```
test> db.smartphones.find()
[
    {
      _id: ObjectId("64084a222063b8732a692d5a"),
      model: 'DEXP G450 One',
      color: 'Blue',
      screen: '5 inch',
      year: '2021'
    }
]
```
Этот документ содержится в коллекции `smartphones`.

- Чтобы увеличить пропускную способность СУБД, при работе с большими данными применяется шардирование - базы разбиваются на части (шарды) и размещаются на разных серверах. Это позволяет сбалансировать нагрузку на них.
- Информация закодирована в формате BSON - это помогает быстро искать нужные данные.
- _Но в базах нет хранимых процедур, триггеров и внешних ключей, поэтому невозможно полностью автоматизировать работу._

СУБД пригодна для использования приложениями, обрабатывающими данные без жёсткой структуры и связей. Базы MongoDB гибкие и масштабируемые, они обеспечивают быструю и надёжную работу программ.
