SELECT
    i_category,
    SUM(ss_quantity) AS total_quantity,
    AVG(ss_sales_price) AS avg_price
FROM store_sales
JOIN item
    ON ss_item_sk = i_item_sk
WHERE i_category = 'Music'
GROUP BY i_category;
