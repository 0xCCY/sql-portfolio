/* 
Summary of problem:
Find the cancellation rate of unbanned user.

Pseudocode:
1. Select and get the total sum of cancelled trips by driver and client.
2. Divide the previous result with the total trips.
3. Constraint the result where `client_id` and `drive_id` is not 'banned' based on the `Users` table

Attemp#1:
* COUNT(id) returns INT, when divided with the sum of cancelled trips which is also of type INT, it ends up ommiting the decimal. 
* The solution is to return decimal in either scenario and CAST everything AS DECIMAL.

Note:
* This is not efficient, the table is too normalized, i.e. the table can be joined and materialized before query.
*/
SELECT
  request_at As 'Day',
  CAST(
    SUM(
      CASE
        WHEN status IN ('cancelled_by_driver', 'cancelled_by_client') then 1.00
        ELSE 0.00
      END
    )/COUNT(id) AS DECIMAL(10, 2)
  ) AS 'Cancellation Rate'
FROM
  Trips T
WHERE
  request_at BETWEEN '2013-10-01' AND '2013-10-03'
  AND client_id NOT IN (
    SELECT
      users_id
    FROM
      Users
    WHERE
      banned='YES'
  )
  AND driver_id NOT IN (
    SELECT
      users_id
    FROM
      Users
    WHERE
      banned='YES'
  )
GROUP BY
  request_at;