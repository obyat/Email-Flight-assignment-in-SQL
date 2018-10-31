SELECT DISTINCT (MgrName)
FROM ManagerSummary
WHERE MgrID = (SELECT MgrId
              FROM (  SELECT MgrId, SUM (empcount) AS maxemp
                        FROM ManagerSummary
                    GROUP BY MgrId
                    ORDER BY SUM (empcount) DESC)
             WHERE ROWNUM = 1);
