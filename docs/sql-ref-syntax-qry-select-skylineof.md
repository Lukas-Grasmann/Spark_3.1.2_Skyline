---
layout: global
title: SKYLINE OF Clause
displayTitle: 
title: SKYLINE OF Clause
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---

### Description

The `SKYLINE OF` clause is used to compute the skyline of the results produced by
`HAVING` based on the specified dimensions. It is executed after the [HAVING] clause (sql-ref-syntax-qry-select-having.html) but before the [ORDER BY](sql-ref-syntax-qry-select-orderby.html)
clause.

### Syntax

```sql
SKYLINE OF [ DISTINCT ] { expression { MIN, MAX, DIFF } [ , ... ] }
```

### Parameters

* **SKYLINE OF**

  Specifies a comma-separated list of skyline dimensions and whether they are minimized (MIN), maximized (MAX) or different (DIFF).
  Optionally, it can be specified whether a skylien dimension is distinct for each dimension. Distinct selection may be completely random.
  Computation follows the definition provided in [Stephan Börzsönyi, Donald Kossmann, Konrad Stocker: The Skyline Operator. ICDE 2001: 421-430].

* **expression**

  Specifies an expression that represents a skyline dimension. Typically is a column but may also be an alias or similar.

* **DISTINCT**

  Specifies that a given skyline should be distinct i.e. data points which are the same for every dimension are eliminated.

* **MIN**

  Specifies that a given skyline dimension should be minimized i.e. a smaller value dominates a bigger value.

* **MAX**

  Specifies that a given skyline dimension should be minimized i.e. a bigger value dominates a smaller value.

* **DIFF**

  Specifies that a given skyline dimension should be different i.e. a if the value on the dimension is different for two tuples, then both can be part of the skyline.

### Examples

TO BE ADDED

### Related Statements

* [SELECT Main](sql-ref-syntax-qry-select.html)
* [WHERE Clause](sql-ref-syntax-qry-select-where.html)
* [GROUP BY Clause](sql-ref-syntax-qry-select-groupby.html)
* [HAVING Clause](sql-ref-syntax-qry-select-having.html)
* [ORDER BY Clause](sql-ref-syntax-qry-select-orderby.html)
* [SORT BY Clause](sql-ref-syntax-qry-select-sortby.html)
* [CLUSTER BY Clause](sql-ref-syntax-qry-select-clusterby.html)
* [DISTRIBUTE BY Clause](sql-ref-syntax-qry-select-distribute-by.html)
* [LIMIT Clause](sql-ref-syntax-qry-select-limit.html)
* [CASE Clause](sql-ref-syntax-qry-select-case.html)
* [PIVOT Clause](sql-ref-syntax-qry-select-pivot.html)
* [LATERAL VIEW Clause](sql-ref-syntax-qry-select-lateral-view.html)
