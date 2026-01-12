erDiagram
    %% RAW LAYER
    RAW_ORDERS ||--o{ RAW_ORDER_ITEMS : contains
    RAW_ORDERS {
        String order_id PK
        String customer_id
        Date order_date
        DateTime order_datetime
        Decimal total_amount
        String metadata_json "JSON"
        String items_json "JSON"
    }

    RAW_SHIPMENTS ||--|| RAW_ORDERS : references
    RAW_SHIPMENTS {
        String shipment_id PK
        String order_id FK
        String warehouse_id FK
        Date shipment_date
        DateTime shipment_datetime
        String route_json "JSON"
        String tracking_json "JSON"
    }

    RAW_INVENTORY {
        String inventory_id PK
        String warehouse_id FK
        String product_id FK
        DateTime snapshot_time
        UInt32 quantity
        String metadata_json "JSON"
    }

    RAW_TRANSPORT {
        String transport_id PK
        String shipment_id FK
        DateTime departure_time
        DateTime arrival_time
        String route_json "JSON"
        String vehicle_info_json "JSON"
    }

    %% STAGE LAYER
    STAGE_ORDERS ||--o{ STAGE_ORDER_ITEMS : contains
    STAGE_ORDERS {
        String order_id PK
        String customer_id FK
        Date order_date
        DateTime order_datetime
        Decimal total_amount
        String payment_method
        String status
    }

    STAGE_SHIPMENTS ||--|| STAGE_ORDERS : references
    STAGE_SHIPMENTS {
        String shipment_id PK
        String order_id FK
        String warehouse_id FK
        DateTime shipment_datetime
        String status
        Decimal weight
    }

    STAGE_INVENTORY_SNAPSHOTS {
        String warehouse_id PK
        String product_id PK
        DateTime snapshot_time PK
        UInt32 quantity
    }

    STAGE_PRODUCT_PRICES {
        String product_id PK
        Date price_date PK
        Decimal price
        String supplier_id FK
    }

    STAGE_CUSTOMER_SEGMENTS {
        String customer_id PK
        String segment
    }

    STAGE_REGIONS_SOURCE {
        UInt64 region_id PK
        UInt64 parent_id "HIERARCHICAL"
        String region_name
        String region_type
    }

    %% MART LAYER
    MART_ORDERS_2024 ||--o{ MART_ORDERS_MERGE : merged
    MART_ORDERS_2023 ||--o{ MART_ORDERS_MERGE : merged
    MART_ORDERS_2022 ||--o{ MART_ORDERS_MERGE : merged

    MART_ORDERS_MERGE {
        String order_id PK
        String customer_id FK
        Date order_date
        Decimal total_amount
    }

    MART_SHIPMENTS_AGG {
        String warehouse_id PK
        Date shipment_date PK
        UInt32 total_shipments
        Decimal total_weight
    }

    MART_INVENTORY_AGG {
        String warehouse_id PK
        String product_id PK
        Date snapshot_date PK
        UInt32 avg_quantity
        UInt32 min_quantity
        UInt32 max_quantity
    }

    %% DICTIONARIES
    DICT_REGIONS_HIERARCHY {
        UInt64 region_id PK
        UInt64 parent_id "HIERARCHICAL"
        String region_name
        String region_type
    }

    DICT_SUPPLIERS {
        UInt64 supplier_id PK
        String supplier_name
        String country
        Float32 rating
    }

    DICT_PRODUCTS {
        UInt64 product_id PK
        String product_name
        String category
        Decimal unit_price
    }

    DICT_WAREHOUSES {
        String warehouse_id PK
        String warehouse_name
        UInt64 region_id FK
        String address
    }

    %% RELATIONSHIPS
    STAGE_ORDERS }o--|| STAGE_CUSTOMER_SEGMENTS : "LEFT JOIN"
    STAGE_ORDERS }o--|| STAGE_PRODUCT_PRICES : "ANY JOIN"
    STAGE_SHIPMENTS }o--|| STAGE_INVENTORY_SNAPSHOTS : "ASOF JOIN"
    STAGE_REGIONS_SOURCE ||--|| DICT_REGIONS_HIERARCHY : "source"
    DICT_WAREHOUSES }o--|| DICT_REGIONS_HIERARCHY : "references"