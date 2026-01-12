ATTACH TABLE _ UUID '856aead6-0258-42b1-970f-3e0fcad2113c'
(
    `ts` DateTime,
    `id` UInt32,
    `name` String,
    `age` UInt8
)
ENGINE = MergeTree
PARTITION BY toDate(ts)
ORDER BY id
SETTINGS index_granularity = 8192
