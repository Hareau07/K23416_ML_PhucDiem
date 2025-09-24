#SELECT * FROM sakila.customer;
#select * from customer where store_id = 2

SELECT 
    c.customer_id AS customerid,
    CONCAT(c.first_name, ' ', c.last_name) AS name,
    COUNT(r.rental_id) AS number_of_total
FROM customer c
JOIN rental r ON c.customer_id = r.customer_id
GROUP BY c.customer_id, name
ORDER BY number_of_total DESC;
