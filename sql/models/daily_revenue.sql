SELECT ord.order_date, COUNT(ord.order_id) AS orders_count, SUM(itm.quantity * itm.unit_price) AS total_revenue
FROM refined_orders ord 
INNER JOIN refined_items itm 
ON ord.order_id = itm.order_id AND ord.ingested_at = itm.ingested_at
WHERE ord.status = 'completed'
GROUP BY ord.order_date