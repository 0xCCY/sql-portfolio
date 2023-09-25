/*
Summary of problems:
Find the top 3 salaries for each department.

Pseudocode:
1. Separate the table into different partition based on `departmentId`
2. Order the table by descending `salary`
3. Select the top 3 salary among each department
4. Join the `Employee` and `Department` table to get names

Attemp#1:
By using the window function `ROW_NUMBER()`, the expected result can be achieved but in cases where there are two similar salary (i.e. Joe and Randy both earns 850000), then the third rank will be ommitted.

To solve this problem, another window function `DENSE_RANK` is required. This function will rank similar values as the same rank.

Notes:
* The keyword `OVER` allows window function to be applied to each partition separately.
* CTE is optional here, a subquery is also possible.
* If the data for each department is large, it is better to do partitioning beforehand via SQL code or directly using SSMS.
*/
WITH
  TopSalary AS (
    SELECT
      Id,
      DepartmentId,
      NAME,
      salary,
      DENSE_RANK() OVER (
        PARTITION BY
          DepartmentId
        ORDER BY
          Salary DESC
      ) AS Ranking
    FROM
      Employee
  )
SELECT
  D.Name AS Department,
  T.Name AS Employee,
  T.salary AS Salary
FROM
  TopSalary AS T
  INNER JOIN Department AS D ON T.DepartmentId=D.Id
WHERE
  /* Writing 1=1 allow simpler toggling of consecutive AND statements*/
  1=1
  AND T.Ranking<=3