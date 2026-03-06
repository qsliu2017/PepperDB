-- JOIN types
CREATE TABLE departments (id int, name text);
INSERT INTO departments VALUES (1, 'engineering');
INSERT INTO departments VALUES (2, 'marketing');
INSERT INTO departments VALUES (3, 'sales');

CREATE TABLE employees (id int, name text, dept_id int);
INSERT INTO employees VALUES (100, 'alice', 1);
INSERT INTO employees VALUES (101, 'bob', 1);
INSERT INTO employees VALUES (102, 'carol', 2);
INSERT INTO employees VALUES (103, 'dave', NULL);

-- INNER JOIN
SELECT e.name, d.name FROM employees e INNER JOIN departments d ON e.dept_id = d.id ORDER BY e.name;

-- LEFT JOIN
SELECT e.name, d.name FROM employees e LEFT JOIN departments d ON e.dept_id = d.id ORDER BY e.name;

-- RIGHT JOIN
SELECT e.name, d.name FROM employees e RIGHT JOIN departments d ON e.dept_id = d.id ORDER BY d.name;

-- FULL OUTER JOIN
SELECT e.name, d.name FROM employees e FULL OUTER JOIN departments d ON e.dept_id = d.id ORDER BY e.name, d.name;

-- CROSS JOIN
CREATE TABLE colors (c text);
INSERT INTO colors VALUES ('red');
INSERT INTO colors VALUES ('blue');
CREATE TABLE sizes (s text);
INSERT INTO sizes VALUES ('S');
INSERT INTO sizes VALUES ('M');
SELECT c, s FROM colors CROSS JOIN sizes ORDER BY c, s;

-- Self join
SELECT a.name AS emp1, b.name AS emp2 FROM employees a INNER JOIN employees b ON a.dept_id = b.dept_id AND a.id < b.id ORDER BY a.name;

-- Multi-table join
SELECT e.name, d.name FROM employees e JOIN departments d ON e.dept_id = d.id WHERE d.name = 'engineering' ORDER BY e.name;

-- JOIN with aggregate
SELECT d.name, COUNT(*) FROM employees e JOIN departments d ON e.dept_id = d.id GROUP BY d.name ORDER BY d.name;
