CREATE DATABASE IF NOT EXISTS logistics
COMMENT 'База данных для домена Logistics & Supply Chain';

-- ============================================
-- RAW LAYER - СЫРЫЕ ДАННЫЕ
-- Демонстрация: MergeTree, JSON колонки, Array колонки
-- ============================================

-- Таблица сырых заказов
CREATE OR REPLACE TABLE logistics.raw_orders
(
    order_id String COMMENT 'Уникальный идентификатор заказа',
    customer_id String COMMENT 'Идентификатор клиента',
    order_date Date COMMENT 'Дата заказа',
    order_datetime DateTime COMMENT 'Дата и время создания заказа',
    total_amount Decimal(10, 2) COMMENT 'Общая сумма заказа',

    -- JSON колонки для демонстрации работы с JSON
    metadata_json String COMMENT 'JSON с метаданными заказа (payment_method, discount, special_instructions)',
    items_json String COMMENT 'JSON массив товаров в заказе',

    -- Array колонки для демонстрации функций работы с массивами
    tags Array(String) COMMENT 'Массив тегов заказа: urgent, vip, express и т.д.',
    product_ids Array(String) COMMENT 'Массив идентификаторов товаров в заказе',
    quantities Array(UInt32) COMMENT 'Массив количеств товаров (соответствует product_ids)',
    prices Array(Decimal(10, 2)) COMMENT 'Массив цен товаров (соответствует product_ids)',
    discount_percentages Array(UInt8) COMMENT 'Массив процентов скидок по товарам',
    delivery_dates Array(Date) COMMENT 'Массив дат доставки (если несколько этапов)',
    status_history Array(String) COMMENT 'История статусов заказа: created, paid, shipped, delivered',
    status_timestamps Array(DateTime) COMMENT 'Временные метки изменения статусов (соответствует status_history)',

    -- Технические поля
    created_at DateTime DEFAULT now() COMMENT 'Время создания записи',
    source String DEFAULT 'api' COMMENT 'Источник данных: api, kafka, airflow etl'
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(order_date)
ORDER BY (order_date, order_id)
COMMENT 'Сырые данные заказов. Демонстрация: MergeTree, JSON, Array колонки';

-- Таблица сырых поставок
CREATE OR REPLACE TABLE logistics.raw_shipments
(
    shipment_id String COMMENT 'Уникальный идентификатор поставки',
    order_id String COMMENT 'Идентификатор заказа (связь с raw_orders)',
    warehouse_id String COMMENT 'Идентификатор склада отправления',
    shipment_date Date COMMENT 'Дата поставки',
    shipment_datetime DateTime COMMENT 'Дата и время отправки',

    -- JSON колонки
    route_json String COMMENT 'JSON с данными маршрута доставки (waypoints, distance, duration)',
    tracking_json String COMMENT 'JSON с трекинг информацией (events, locations, timestamps)',

    -- Array колонки
    package_ids Array(String) COMMENT 'Массив идентификаторов упаковок в поставке',
    package_weights Array(Float32) COMMENT 'Массив весов упаковок (соответствует package_ids)',
    tracking_numbers Array(String) COMMENT 'Массив трекинг номеров для отслеживания',
    checkpoints Array(String) COMMENT 'Массив точек контроля: warehouse, transit, delivery, customs',
    checkpoint_times Array(DateTime) COMMENT 'Временные метки прохождения точек контроля (соответствует checkpoints)',
    vehicle_ids Array(String) COMMENT 'Массив идентификаторов транспортных средств (если перегрузка)',
    driver_ids Array(String) COMMENT 'Массив идентификаторов водителей (если смена водителя)',

    -- Технические поля
    created_at DateTime DEFAULT now() COMMENT 'Время создания записи',
    source String DEFAULT 'api' COMMENT 'Источник данных'
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(shipment_date)
ORDER BY (shipment_date, shipment_id)
COMMENT 'Сырые данные поставок. Демонстрация: MergeTree, JSON, Array колонки';

-- Таблица сырого инвентаря
CREATE OR REPLACE TABLE logistics.raw_inventory
(
    inventory_id String COMMENT 'Уникальный идентификатор записи инвентаря',
    warehouse_id String COMMENT 'Идентификатор склада',
    product_id String COMMENT 'Идентификатор товара',
    snapshot_time DateTime COMMENT 'Время снимка состояния инвентаря',
    quantity UInt32 COMMENT 'Количество товара на складе',

    -- JSON колонки
    metadata_json String COMMENT 'JSON с дополнительными метаданными (batch_info, quality_control)',

    -- Array колонки
    location_codes Array(String) COMMENT 'Массив кодов локаций на складе: A-1-2, B-3-4 (если товар в нескольких местах)',
    batch_numbers Array(String) COMMENT 'Массив номеров партий товара',
    expiry_dates Array(Date) COMMENT 'Массив сроков годности по партиям (соответствует batch_numbers)',
    supplier_ids Array(String) COMMENT 'Массив идентификаторов поставщиков (если товар из разных партий)',
    cost_prices Array(Decimal(10, 2)) COMMENT 'Массив себестоимостей по партиям (соответствует batch_numbers)',

    -- Технические поля
    created_at DateTime DEFAULT now() COMMENT 'Время создания записи',
    source String DEFAULT 'api' COMMENT 'Источник данных'
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(snapshot_time)
ORDER BY (warehouse_id, product_id, snapshot_time)
COMMENT 'Сырые данные инвентаря. Демонстрация: MergeTree, JSON, Array колонки, временные ряды';

-- Таблица сырого транспорта
CREATE OR REPLACE TABLE logistics.raw_transport
(
    transport_id String COMMENT 'Уникальный идентификатор транспортировки',
    shipment_id String COMMENT 'Идентификатор поставки (связь с raw_shipments)',
    departure_time DateTime COMMENT 'Время отправления',
    arrival_time DateTime COMMENT 'Время прибытия',

    -- JSON колонки
    route_json String COMMENT 'JSON с детальной информацией о маршруте (coordinates, stops, traffic)',
    vehicle_info_json String COMMENT 'JSON с информацией о транспортном средстве (type, capacity, driver)',

    -- Технические поля
    created_at DateTime DEFAULT now() COMMENT 'Время создания записи',
    source String DEFAULT 'api' COMMENT 'Источник данных'
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(departure_time)
ORDER BY (shipment_id, departure_time)
COMMENT 'Сырые данные транспортировки. Демонстрация: MergeTree, JSON колонки';



-- ============================================
-- STAGE LAYER - ОЧИЩЕННЫЕ ДАННЫЕ
-- Демонстрация: ReplacingMergeTree, CollapsingMergeTree,
-- VersionedCollapsingMergeTree, таблицы для JOIN
-- ============================================

USE logistics;

-- Очищенные заказы (ReplacingMergeTree для дедупликации)
CREATE OR REPLACE TABLE logistics.stage_orders
(
    order_id String COMMENT 'Уникальный идентификатор заказа',
    customer_id String COMMENT 'Идентификатор клиента',
    order_date Date COMMENT 'Дата заказа',
    order_datetime DateTime COMMENT 'Дата и время создания заказа',
    total_amount Decimal(10, 2) COMMENT 'Общая сумма заказа',
    payment_method String COMMENT 'Способ оплаты: card, cash, transfer',
    status String COMMENT 'Текущий статус заказа: created, paid, shipped, delivered, cancelled',

    -- Array колонки (очищенные и валидированные)
    tags Array(String) COMMENT 'Массив тегов заказа (валидированные значения)',
    product_ids Array(String) COMMENT 'Массив идентификаторов товаров',
    quantities Array(UInt32) COMMENT 'Массив количеств товаров',
    prices Array(Decimal(10, 2)) COMMENT 'Массив цен товаров',
    status_history Array(String) COMMENT 'История статусов заказа',
    status_timestamps Array(DateTime) COMMENT 'Временные метки статусов',

    -- Технические поля
    updated_at DateTime DEFAULT now() COMMENT 'Время последнего обновления',
    version UInt64 DEFAULT 1 COMMENT 'Версия записи для ReplacingMergeTree'
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(order_date)
ORDER BY (order_id)
SETTINGS deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Очищенные данные заказов. Демонстрация: ReplacingMergeTree, дедупликация, TTL';

-- Очищенные поставки (CollapsingMergeTree для изменяемых данных)
CREATE OR REPLACE TABLE  logistics.stage_shipments
(
    shipment_id String COMMENT 'Уникальный идентификатор поставки',
    order_id String COMMENT 'Идентификатор заказа',
    warehouse_id String COMMENT 'Идентификатор склада',
    shipment_datetime DateTime COMMENT 'Дата и время отправки',
    status String COMMENT 'Статус поставки: pending, in_transit, delivered, returned',
    weight Decimal(10, 2) COMMENT 'Общий вес поставки в кг',

    -- Array колонки
    package_ids Array(String) COMMENT 'Массив идентификаторов упаковок',
    package_weights Array(Float32) COMMENT 'Массив весов упаковок',
    tracking_numbers Array(String) COMMENT 'Массив трекинг номеров',
    checkpoints Array(String) COMMENT 'Массив точек контроля',
    checkpoint_times Array(DateTime) COMMENT 'Временные метки точек контроля',

    -- Поле для CollapsingMergeTree
    sign Int8 COMMENT 'Знак для CollapsingMergeTree: 1 - вставка, -1 - удаление',

    -- Технические поля
    updated_at DateTime DEFAULT now() COMMENT 'Время последнего обновления'
)
ENGINE = CollapsingMergeTree(sign)
PARTITION BY toYYYYMM(shipment_datetime)
ORDER BY (shipment_id)
SETTINGS deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Очищенные данные поставок. Демонстрация: CollapsingMergeTree для изменяемых данных';

-- Снимки инвентаря для ASOF JOIN
CREATE OR REPLACE TABLE logistics.stage_inventory_snapshots
(
    warehouse_id String COMMENT 'Идентификатор склада',
    product_id String COMMENT 'Идентификатор товара',
    snapshot_time DateTime COMMENT 'Время снимка (для ASOF JOIN)',
    quantity UInt32 COMMENT 'Количество товара на момент снимка',
    reserved_quantity UInt32 COMMENT 'Зарезервированное количество',
    available_quantity UInt32 COMMENT 'Доступное количество (quantity - reserved_quantity)',

    -- Технические поля
    created_at DateTime DEFAULT now() COMMENT 'Время создания записи'
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(snapshot_time)
ORDER BY (warehouse_id, product_id, snapshot_time)
COMMENT 'Снимки инвентаря для ASOF JOIN. Демонстрация: временные ряды, ASOF JOIN';

-- Цены товаров для ANY JOIN (с дубликатами ключей)
CREATE OR REPLACE TABLE logistics.stage_product_prices
(
    product_id String COMMENT 'Идентификатор товара',
    price_date Date COMMENT 'Дата действия цены',
    price Decimal(10, 2) COMMENT 'Цена товара',
    supplier_id String COMMENT 'Идентификатор поставщика (может быть несколько для одного товара)',
    currency String DEFAULT 'RUB' COMMENT 'Валюта цены',

    -- Технические поля
    created_at DateTime DEFAULT now() COMMENT 'Время создания записи'
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(price_date)
ORDER BY (product_id, price_date, supplier_id)
COMMENT 'Цены товаров. Демонстрация: ANY JOIN (дубликаты ключей product_id)';

-- Сегменты клиентов для LEFT/RIGHT JOIN
CREATE OR REPLACE TABLE logistics.stage_customer_segments
(
    customer_id String COMMENT 'Идентификатор клиента',
    segment String COMMENT 'Сегмент клиента: vip, regular, new, inactive',
    segment_score Decimal(5, 2) COMMENT 'Оценка сегмента (0-100)',
    assigned_date Date COMMENT 'Дата назначения сегмента',

    -- Технические поля
    updated_at DateTime DEFAULT now() COMMENT 'Время последнего обновления'
)
ENGINE = MergeTree()
ORDER BY (customer_id)
COMMENT 'Сегменты клиентов. Демонстрация: LEFT JOIN, RIGHT JOIN (не все клиенты имеют сегмент)';

-- Источник данных для иерархического словаря регионов
CREATE OR REPLACE TABLE logistics.stage_regions_source
(
    region_id UInt64 COMMENT 'Уникальный идентификатор региона',
    parent_id UInt64 COMMENT 'Идентификатор родительского региона (0 для корневых элементов)',
    region_name String COMMENT 'Название региона',
    region_type String COMMENT 'Тип региона: country, region, city',
    region_code String COMMENT 'Код региона (ISO, внутренний код)',
    population UInt32 COMMENT 'Население региона',
    area_km2 Float64 COMMENT 'Площадь региона в квадратных километрах',

    -- Технические поля
    updated_at DateTime DEFAULT now() COMMENT 'Время последнего обновления'
)
ENGINE = MergeTree()
ORDER BY (region_id)
COMMENT 'Источник данных для иерархического словаря регионов. Структура: Страна -> Регион -> Город';

-- Справочник складов
CREATE OR REPLACE TABLE logistics.stage_warehouses_source
(
    warehouse_id String COMMENT 'Уникальный идентификатор склада',
    warehouse_name String COMMENT 'Название склада',
    region_id UInt64 COMMENT 'Идентификатор региона (связь с stage_regions_source)',
    address String COMMENT 'Адрес склада',
    capacity_m3 Float64 COMMENT 'Емкость склада в кубических метрах',
    is_active UInt8 COMMENT 'Флаг активности склада: 1 - активен, 0 - неактивен',

    -- Технические поля
    created_at DateTime DEFAULT now() COMMENT 'Время создания записи',
    updated_at DateTime DEFAULT now() COMMENT 'Время последнего обновления'
)
ENGINE = MergeTree()
ORDER BY (warehouse_id)
COMMENT 'Справочник складов';

-- Справочник поставщиков
CREATE OR REPLACE TABLE logistics.stage_suppliers_source
(
    supplier_id String COMMENT 'Уникальный идентификатор поставщика',
    supplier_name String COMMENT 'Название поставщика',
    country String COMMENT 'Страна поставщика',
    rating Float32 COMMENT 'Рейтинг поставщика (0-10)',
    contact_email String COMMENT 'Email для связи',

    -- Технические поля
    created_at DateTime DEFAULT now() COMMENT 'Время создания записи',
    updated_at DateTime DEFAULT now() COMMENT 'Время последнего обновления'
)
ENGINE = MergeTree()
ORDER BY (supplier_id)
COMMENT 'Справочник поставщиков';

-- Справочник товаров
CREATE OR REPLACE TABLE logistics.stage_products_source
(
    product_id String COMMENT 'Уникальный идентификатор товара',
    product_name String COMMENT 'Название товара',
    category String COMMENT 'Категория товара: electronics, clothing, food, etc.',
    unit_price Decimal(10, 2) COMMENT 'Цена за единицу товара',
    weight_kg Float32 COMMENT 'Вес единицы товара в кг',
    volume_m3 Float32 COMMENT 'Объем единицы товара в м³',

    -- Технические поля
    created_at DateTime DEFAULT now() COMMENT 'Время создания записи',
    updated_at DateTime DEFAULT now() COMMENT 'Время последнего обновления'
)
ENGINE = MergeTree()
ORDER BY (product_id)
COMMENT 'Справочник товаров';


-- ============================================
-- MART LAYER - АНАЛИТИЧЕСКИЕ ВИТРИНЫ
-- Демонстрация: SummingMergeTree, AggregatingMergeTree,
-- Nested колонки для sumMap, Merge таблицы
-- ============================================

USE logistics;

-- Витрина заказов с Nested структурами для sumMap
CREATE OR REPLACE TABLE logistics.mart_order_items_nested
(
    order_date Date COMMENT 'Дата заказа',
    customer_id String COMMENT 'Идентификатор клиента',

    -- Nested структура для агрегации продуктов через sumMap
    product_metrics Nested(
        product_id String,
        quantity UInt32,
        amount Decimal(10, 2)
    ) COMMENT 'Nested структура для агрегации продуктов. Демонстрация: sumMap',

    -- Nested структура для агрегации тегов
    tag_metrics Nested(
        tag String,
        count UInt32
    ) COMMENT 'Nested структура для агрегации тегов через sumMap',

    -- Nested структура для агрегации категорий
    category_metrics Nested(
        category String,
        count UInt32
    ) COMMENT 'Nested структура для агрегации категорий через sumMap',

    -- Nested структура для агрегации статусов
    status_metrics Nested(
        status String,
        count UInt32
    ) COMMENT 'Nested структура для агрегации статусов через sumMap'
)
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(order_date)
ORDER BY (order_date, customer_id)
COMMENT 'Витрина заказов с Nested структурами. Демонстрация: SummingMergeTree, sumMap, Nested колонки';

-- Витрина поставок с Nested структурами
CREATE OR REPLACE TABLE logistics.mart_shipment_metrics_nested
(
    shipment_date Date COMMENT 'Дата поставки',
    warehouse_id String COMMENT 'Идентификатор склада',

    -- Nested структура для агрегации весов продуктов
    product_weights Nested(
        product_id String,
        weight Float32
    ) COMMENT 'Nested структура для агрегации весов продуктов через sumMap',

    -- Nested структура для агрегации точек контроля
    checkpoint_metrics Nested(
        checkpoint String,
        count UInt32
    ) COMMENT 'Nested структура для агрегации точек контроля через sumMap',

    -- Nested структура для агрегации использования транспорта
    vehicle_metrics Nested(
        vehicle_id String,
        usage_count UInt32
    ) COMMENT 'Nested структура для агрегации использования транспорта через sumMap',

    -- Nested структура для агрегации рейсов водителей
    driver_metrics Nested(
        driver_id String,
        trip_count UInt32
    ) COMMENT 'Nested структура для агрегации рейсов водителей через sumMap'
)
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(shipment_date)
ORDER BY (shipment_date, warehouse_id)
COMMENT 'Витрина поставок с Nested структурами. Демонстрация: SummingMergeTree, sumMap';

-- Агрегированная витрина поставок (SummingMergeTree без Nested)
CREATE OR REPLACE TABLE logistics.mart_shipments_agg
(
    warehouse_id String COMMENT 'Идентификатор склада',
    shipment_date Date COMMENT 'Дата поставки',
    total_shipments UInt32 COMMENT 'Общее количество поставок (автосуммирование)',
    total_weight Decimal(10, 2) COMMENT 'Общий вес всех поставок (автосуммирование)',
    total_packages UInt32 COMMENT 'Общее количество упаковок (автосуммирование)',

    -- Технические поля
    updated_at DateTime DEFAULT now() COMMENT 'Время последнего обновления'
)
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(shipment_date)
ORDER BY (shipment_date, warehouse_id)
COMMENT 'Агрегированная витрина поставок. Демонстрация: SummingMergeTree автосуммирование';

-- Агрегированная витрина инвентаря (AggregatingMergeTree)
CREATE OR REPLACE TABLE logistics.mart_inventory_agg
(
    warehouse_id String COMMENT 'Идентификатор склада',
    product_id String COMMENT 'Идентификатор товара',
    snapshot_date Date COMMENT 'Дата снимка',
    avg_quantity AggregateFunction(avg, UInt32) COMMENT 'Среднее количество (агрегат)',
    min_quantity SimpleAggregateFunction(min, UInt32) COMMENT 'Минимальное количество (агрегат)',
    max_quantity SimpleAggregateFunction(max, UInt32) COMMENT 'Максимальное количество (агрегат)',
    sum_quantity SimpleAggregateFunction(sum, UInt64) COMMENT 'Суммарное количество (агрегат)',

    -- Технические поля
    updated_at DateTime DEFAULT now() COMMENT 'Время последнего обновления'
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(snapshot_date)
ORDER BY (warehouse_id, product_id, snapshot_date)
COMMENT 'Агрегированная витрина инвентаря. Демонстрация: AggregatingMergeTree, SimpleAggregateFunction';

-- Отдельные таблицы по годам для Merge таблицы
CREATE OR REPLACE TABLE logistics.mart_orders_2024
(
    order_id String COMMENT 'Уникальный идентификатор заказа',
    customer_id String COMMENT 'Идентификатор клиента',
    order_date Date COMMENT 'Дата заказа',
    total_amount Decimal(10, 2) COMMENT 'Общая сумма заказа',
    status String COMMENT 'Статус заказа',

    -- Технические поля
    created_at DateTime DEFAULT now() COMMENT 'Время создания записи'
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(order_date)
ORDER BY (order_date, order_id)
COMMENT 'Заказы за 2024 год. Используется в Merge таблице';

CREATE OR REPLACE TABLE logistics.mart_orders_2023
(
    order_id String COMMENT 'Уникальный идентификатор заказа',
    customer_id String COMMENT 'Идентификатор клиента',
    order_date Date COMMENT 'Дата заказа',
    total_amount Decimal(10, 2) COMMENT 'Общая сумма заказа',
    status String COMMENT 'Статус заказа',

    -- Технические поля
    created_at DateTime DEFAULT now() COMMENT 'Время создания записи'
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(order_date)
ORDER BY (order_date, order_id)
COMMENT 'Заказы за 2023 год. Используется в Merge таблице';

CREATE OR REPLACE TABLE logistics.mart_orders_2022
(
    order_id String COMMENT 'Уникальный идентификатор заказа',
    customer_id String COMMENT 'Идентификатор клиента',
    order_date Date COMMENT 'Дата заказа',
    total_amount Decimal(10, 2) COMMENT 'Общая сумма заказа',
    status String COMMENT 'Статус заказа',

    -- Технические поля
    created_at DateTime DEFAULT now() COMMENT 'Время создания записи'
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(order_date)
ORDER BY (order_date, order_id)
COMMENT 'Заказы за 2022 год. Используется в Merge таблице';

-- Merge таблица для объединения исторических данных
CREATE OR REPLACE TABLE logistics.mart_orders_merge
(
    order_id String COMMENT 'Уникальный идентификатор заказа',
    customer_id String COMMENT 'Идентификатор клиента',
    order_date Date COMMENT 'Дата заказа',
    total_amount Decimal(10, 2) COMMENT 'Общая сумма заказа',
    status String COMMENT 'Статус заказа',
    created_at DateTime COMMENT 'Время создания записи'
)
ENGINE = Merge('logistics', '^mart_orders_\\d{4}$')
COMMENT 'Merge таблица для объединения заказов по годам. Демонстрация: Merge Engine, объединение нескольких таблиц';



-- ============================================
-- MATERIALIZED VIEWS
-- Демонстрация: Real Time агрегация, заполнение витрин
-- ============================================

USE logistics;

-- Materialized View для заполнения Nested витрины заказов
CREATE MATERIALIZED VIEW IF NOT EXISTS logistics.mart_order_items_nested_mv
TO logistics.mart_order_items_nested
AS
SELECT
    order_date,
    customer_id,
    -- Nested структура для продуктов
    product_ids as `product_metrics.product_id`,
    quantities as `product_metrics.quantity`,
    arrayMap((p, q) -> p * toDecimal32(q, 0), prices, quantities) as `product_metrics.amount`,
    -- Nested структура для тегов
    tags as `tag_metrics.tag`,
    arrayMap(t -> 1, tags) as `tag_metrics.count`,
    -- Nested структура для статуса
    [status] as `status_metrics.status`,
    [1] as `status_metrics.count`,
    -- Пустые массивы для категорий с явным указанием типа
    emptyArrayString() as `category_metrics.category`,
    emptyArrayUInt32() as `category_metrics.count`
FROM logistics.stage_orders;

-- Materialized View для заполнения агрегированной витрины поставок
CREATE MATERIALIZED VIEW IF NOT EXISTS logistics.mart_shipments_agg_mv
TO logistics.mart_shipments_agg
AS
SELECT
    toDate(shipment_datetime) as shipment_date,
    warehouse_id,
    count() as total_shipments,
    sum(weight) as total_weight,
    sum(length(package_ids)) as total_packages
FROM logistics.stage_shipments
WHERE sign > 0  -- Только активные записи для CollapsingMergeTree
GROUP BY shipment_date, warehouse_id;

-- Materialized View для заполнения агрегированной витрины поставок
CREATE MATERIALIZED VIEW IF NOT EXISTS logistics.mart_shipments_agg_mv
TO logistics.mart_shipments_agg
AS
SELECT
    toDate(shipment_datetime) as shipment_date,
    warehouse_id,
    count() as total_shipments,
    sum(weight) as total_weight,
    sum(length(package_ids)) as total_packages
FROM logistics.stage_shipments
WHERE sign > 0  -- Только активные записи для CollapsingMergeTree
GROUP BY shipment_date, warehouse_id
COMMENT 'Materialized View для заполнения агрегированной витрины поставок. Демонстрация: SummingMergeTree автосуммирование';

DROP VIEW IF EXISTS logistics.mart_inventory_agg_mv;

-- Materialized View для заполнения агрегированной витрины инвентаря
CREATE MATERIALIZED VIEW IF NOT EXISTS logistics.mart_inventory_agg_mv
TO logistics.mart_inventory_agg
AS
SELECT
    warehouse_id,
    product_id,
    toDate(snapshot_time) as snapshot_date,
    avgState(quantity) as avg_quantity,  -- Это создаст AggregateFunction
    min(quantity) as min_quantity,        -- Это создаст SimpleAggregateFunction
    max(quantity) as max_quantity,         -- Это создаст SimpleAggregateFunction
    sum(quantity) as sum_quantity         -- Это создаст SimpleAggregateFunction
FROM logistics.stage_inventory_snapshots
GROUP BY warehouse_id, product_id, snapshot_date
COMMENT 'Materialized View для заполнения агрегированной витрины инвентаря. Демонстрация: AggregatingMergeTree с State функциями';


select warehouse_id, product_id from logistics.mart_inventory_agg;
-- ============================================
-- PROJECTIONS
-- Демонстрация: оптимизация запросов, предрасчеты
-- ============================================

USE logistics;

-- Добавление Projection к таблице заказов для оптимизации запросов по клиентам
ALTER TABLE logistics.stage_orders
ADD PROJECTION IF NOT EXISTS orders_by_customer
(
    SELECT
        customer_id,
        order_date,
        count() as order_count,
        sum(total_amount) as total_spent,
        uniqExact(product_ids) as unique_products_count
    GROUP BY customer_id, order_date
);

-- Добавление Projection к таблице поставок для оптимизации запросов по складам
ALTER TABLE logistics.stage_shipments
ADD PROJECTION IF NOT EXISTS shipments_by_warehouse
(
    SELECT
        warehouse_id,
        toDate(shipment_datetime) as shipment_date,
        count() as shipment_count,
        sum(weight) as total_weight,
        uniqExact(order_id) as unique_orders_count
    GROUP BY warehouse_id, shipment_date
);

-- Materialize существующие Projections (опционально, раскомментировать при необходимости)
-- ALTER TABLE logistics.stage_orders MATERIALIZE PROJECTION orders_by_customer;
-- ALTER TABLE logistics.stage_shipments MATERIALIZE PROJECTION shipments_by_warehouse;


-- ============================================
-- DICTIONARIES (SQL)
-- Демонстрация: иерархический словарь, простые словари
-- ============================================

USE logistics;

-- Иерархический словарь регионов
CREATE OR REPLACE DICTIONARY logistics.dict_regions_hierarchy
(
    region_id UInt64,
    parent_id UInt64 HIERARCHICAL,
    region_name String,
    region_type String,
    region_code String,
    population UInt32,
    area_km2 Float64
)
PRIMARY KEY region_id
SOURCE(CLICKHOUSE(
    DB 'logistics'
    TABLE 'stage_regions_source'
))
LAYOUT(HASHED())
LIFETIME(MIN 300 MAX 600)
COMMENT 'Иерархический словарь регионов. Демонстрация: HIERARCHICAL layout, навигация по дереву (Страна -> Регион -> Город)';

-- Словарь поставщиков (FLAT layout)
CREATE OR REPLACE DICTIONARY logistics.dict_suppliers
(
    supplier_id String,
    supplier_name String,
    country String,
    rating Float32,
    contact_email String
)
PRIMARY KEY supplier_id
SOURCE(CLICKHOUSE(
    DB 'logistics'
    TABLE 'stage_suppliers_source'
))
LAYOUT(HASHED())
LIFETIME(MIN 300 MAX 600)
COMMENT 'Словарь поставщиков. Демонстрация: FLAT layout для простых справочников';

-- Словарь товаров (CACHE layout для больших справочников)
CREATE OR REPLACE DICTIONARY logistics.dict_products
(
    product_id String,
    product_name String,
    category String,
    unit_price Decimal(10, 2),
    weight_kg Float32,
    volume_m3 Float32
)
PRIMARY KEY product_id
SOURCE(CLICKHOUSE(
    DB 'logistics'
    TABLE 'stage_products_source'
))
LAYOUT(HASHED())
LIFETIME(MIN 300 MAX 600)
COMMENT 'Словарь товаров. Демонстрация: CACHE layout для оптимизации памяти при больших справочниках';

-- Словарь складов (FLAT layout)
CREATE OR REPLACE DICTIONARY logistics.dict_warehouses
(
    warehouse_id String,
    warehouse_name String,
    region_id UInt64,
    address String,
    capacity_m3 Float64,
    is_active UInt8
)
PRIMARY KEY warehouse_id
SOURCE(CLICKHOUSE(
    DB 'logistics'
    TABLE 'stage_warehouses_source'
))
LAYOUT(HASHED())
LIFETIME(MIN 300 MAX 600)
COMMENT 'Словарь складов. Демонстрация: FLAT layout, связь с иерархическим словарем регионов';