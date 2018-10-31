CREATE VIEW ManagerSummary(DeptName, MgrName, MgrID, MgrSalary, EmpCount) AS

SELECT D.dname, D.managerid, E.ename, E.salary, COUNT(W.eid)
FROM dept D, emp E, works W
WHERE D.managerid = E.eid AND D.did = W.did
GROUP BY D.did, D.dname, D.managerid, E.ename, E.salary;
