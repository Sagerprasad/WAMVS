SELECT
    i_category,
    i_class,
    SUM(ss_ext_sales_price) AS revenue
FROM store_sales
JOIN item
    ON ss_item_sk = i_item_sk
GROUP BY i_category, i_class
ORDER BY revenue DESC;
