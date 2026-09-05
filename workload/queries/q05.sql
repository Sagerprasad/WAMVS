SELECT
    d_year,
    SUM(ss_ext_sales_price) AS revenue
FROM store_sales
JOIN date_dim
    ON ss_sold_date_sk = d_date_sk
WHERE d_year BETWEEN 1999 AND 2001
GROUP BY d_year
ORDER BY d_year;
