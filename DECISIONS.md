# DECISIONS.md

1. **Indexes.** `employees`: unique `emp_code` (identity, race-safe create); `department, joined_on` (summary/trend with a department); `joined_on` (summary without one). `attendance_logs`: unique `emp_code, date` (one record per day, race-safe punch-in, PATCH lookup, per-employee month, the `$lookup` from employees); `date -1, emp_code 1` (list sort and month ranges); `emp_code, punch_in -1` (punch-out finds the latest open record). I rejected an index on `status`: low cardinality, and the date index already serves the sort.

2. **Punch-in race.** Both requests compute the same `(emp_code, date)` and call `insert_one`. The unique index lets exactly one write succeed; the other gets `DuplicateKeyError`, which I return as 409. There is no check-then-insert.

3. **Ties.** `$setWindowFields` with `$rank` gives 1, 2, 2, 4, and I filter `rank <= limit` after ranking. So both tied employees are returned, even if that is more than `limit` rows, ordered by `emp_code`.

4. **Headcount.** The pipeline starts from `employees` (`joined_on <= month end`), `$lookup`s the month's logs and `$unwind`s with `preserveNullAndEmptyArrays`, so people with zero logs still reach `$group`.

5. **100x data.** Pre-aggregate daily per-department counters on write, and shard `attendance_logs` by `emp_code`.
