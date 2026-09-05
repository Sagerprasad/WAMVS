SELECT
    i_category,
    d_year,
    COUNT(*) AS transactions,
    SUM(ss_ext_sales_price) AS revenue
FROM store_sales
JOIN item
    ON ss_item_sk = i_item_sk
JOIN date_dim
    ON ss_sold_date_sk = d_date_sk
WHERE i_category IN ('Music', 'Books', 'Sports')
GROUP BY i_category, d_year
ORDER BY d_year, revenue DESC;
