# Employee Risk Analytics Pipeline using Databricks (Bronze → Silver → Gold)

## 1. Project Title

**Employee Risk Analytics Pipeline using Databricks Medallion Architecture**

---

## 2. Project Objective

The objective of this project is to build an **end-to-end batch data pipeline in Databricks** that ingests employee enrollment data from a source file, processes it through **Bronze, Silver, and Gold layers**, and generates a final **risk summary report** for analytics.

This project simulates a real-world **Benefits Administration / HR Analytics use case**, where employee records are regularly received as batch files and must be:

- ingested automatically
- updated/inserted into a raw data table
- transformed based on business rules
- aggregated into reporting-ready output

---

## 3. Business Use Case

In a benefits or HR domain, employee data is frequently received from source systems such as:

- HR systems
- enrollment platforms
- benefits vendors
- payroll files

These files may contain:

- new employees
- updated employee records
- enrollment status changes
- health-related risk indicators

The business needs a pipeline to identify:

- which employees are active
- which employees are high risk
- how many employees fall into each risk group

This project solves that problem using **Databricks + Delta Lake + SQL + PySpark**.

---

## 4. Architecture Overview

This project follows the **Medallion Architecture** pattern:

```text
Source CSV File
      ↓
Bronze Layer → employee_data
      ↓
Silver Layer → employee_cleaned
      ↓
Gold Layer → risk_summary
```

---

## 5. Layer-wise Design

### Bronze Layer — Raw Ingestion

**Table:**
```sql
rohan_catalog.benefits_demo.employee_data
```

**Purpose:**
The Bronze layer stores **raw employee data** as received from source files.

**Key Responsibilities:**
- read source CSV file
- load incoming employee records
- perform **MERGE (Upsert)** into target table
- handle:
  - new records → INSERT
  - existing records → UPDATE
- store metadata columns for auditability

**Metadata Columns Added:**
- `LoadTimestamp` → when the row was loaded
- `SourceFile` → which file the row came from

---

### Silver Layer — Cleaned / Business-Ready Data

**Table:**
```sql
rohan_catalog.benefits_demo.employee_cleaned
```

**Purpose:**
The Silver layer applies business rules and prepares cleaned employee data for reporting.

**Business Rules Applied:**
- keep only employees where:

```text
EnrollmentStatus = 'Active'
```

- derive:

```text
RiskCategory
```

**Risk Logic:**
- if `Smoking = 'Y'` OR `Diabetic = 'Y'` → **High Risk**
- else → **Low Risk**

---

### Gold Layer — Aggregated Reporting

**Table:**
```sql
rohan_catalog.benefits_demo.risk_summary
```

**Purpose:**
The Gold layer provides a final summary for business reporting and dashboards.

**Output Example:**

| RiskCategory | TotalEmployees |
|---|---:|
| High Risk | 4 |
| Low Risk | 7 |

This is the final analytics-ready table.

---

## 6. Project Folder / Notebook Structure

In Databricks Workspace, the project is organized into **3 notebooks**:

```text
Rohan_Benefits_Project
 ├── 01_Bronze_Batch_Input_Upsert
 ├── 02_Silver_Transformation
 └── 03_Gold_Aggregation
```

---

## 7. Source Data Schema

The source employee batch file contains the following columns:

```csv
EmpID,Name,Age,Gender,Department,Salary,Smoking,Diabetic,EnrollmentStatus
```

### Column Definitions

| Column | Description |
|---|---|
| EmpID | Employee unique ID |
| Name | Employee name |
| Age | Employee age |
| Gender | Gender |
| Department | Department name |
| Salary | Employee salary |
| Smoking | Y/N smoking indicator |
| Diabetic | Y/N diabetic indicator |
| EnrollmentStatus | Active / Inactive enrollment status |

---

## 8. Source File Ingestion

The source batch file is uploaded into a **Unity Catalog Volume**.

**Volume Path:**
```text
/Volumes/rohan_catalog/benefits_demo/input_files/
```

**Example Files:**
```text
employee_batch_1.csv
employee_batch_2.csv
employee_batch_3.csv
```

Each file represents a **new batch of incoming employee data**.

---

## 9. Bronze Layer Logic (Notebook 1)

**Notebook:**
```text
01_Bronze_Batch_Input_Upsert
```

**Technologies Used:**
- PySpark
- Databricks SQL
- Delta Lake MERGE

**Processing Steps:**
1. Read source CSV file from Unity Catalog Volume
2. Add metadata columns:
   - `LoadTimestamp`
   - `SourceFile`
3. Create temp view:
```sql
updates
```
4. MERGE into Bronze table:
```sql
rohan_catalog.benefits_demo.employee_data
```

### Bronze MERGE Logic

**If employee already exists:**
```text
UPDATE existing row
```

**If employee is new:**
```text
INSERT new row
```

This is called:

**UPSERT**

---

## 10. Silver Layer Logic (Notebook 2)

**Notebook:**
```text
02_Silver_Transformation
```

**Technologies Used:**
- PySpark
- Spark SQL functions

**Processing Steps:**
1. Read Bronze table
2. Filter only active employees
3. Derive `RiskCategory`
4. Save transformed output into Silver table

### Silver Transformation Rule

```python
if Smoking == 'Y' or Diabetic == 'Y':
    RiskCategory = 'High Risk'
else:
    RiskCategory = 'Low Risk'
```

**Silver Output Example:**

| EmpID | Name | Smoking | Diabetic | RiskCategory |
|---|---|---|---|---|
| 101 | Rohan | Y | N | High Risk |
| 104 | Pooja | N | N | Low Risk |

---

## 11. Gold Layer Logic (Notebook 3)

**Notebook:**
```text
03_Gold_Aggregation
```

**Technologies Used:**
- PySpark
- Aggregation functions

**Processing Steps:**
1. Read Silver table
2. Group by `RiskCategory`
3. Count total employees
4. Save final output into Gold table

### Gold Aggregation Logic

```python
groupBy("RiskCategory").count()
```

**Gold Output Example:**

| RiskCategory | TotalEmployees |
|---|---:|
| High Risk | 4 |
| Low Risk | 3 |

This output is **dashboard-ready** and can be used in:

- Power BI
- Tableau
- Databricks SQL Dashboard

---

## 12. Workflow / Job Orchestration

The project is automated using a **Databricks Job Workflow**.

**Job Name:**
```text
Benefits_Demo_Pipeline
```

**Task Flow:**
```text
01_Bronze_Batch_Input_Upsert
          ↓
02_Silver_Transformation
          ↓
03_Gold_Aggregation
```

**Task Names:**

| Task Name | Notebook |
|---|---|
| bronze_upsert | 01_Bronze_Batch_Input_Upsert |
| silver_transform | 02_Silver_Transformation |
| gold_aggregate | 03_Gold_Aggregation |

---

## 13. Batch Processing Capability

This project supports **batch ingestion**.

That means each new source file can contain:

- new employees
- updated employees
- status changes

**Example:**
```text
employee_batch_3.csv
```

can include:

- employee already existing → update
- employee not existing → insert

This makes the project realistic for **production-like file ingestion**.

---

## 14. Testing Scenarios Covered

This project was tested using multiple batch files.

**Scenarios tested:**
- insert new employee
- update existing employee
- active employee filtering
- inactive employee exclusion
- high-risk employee identification
- low-risk employee identification
- final summary validation

---

## 15. Challenges Faced and Resolutions

During project implementation, several Databricks-specific issues were encountered and resolved.

### Issue 1 — DBFS Disabled
**Problem:** Public DBFS root was disabled.

**Resolution:** Used **Unity Catalog Volumes** instead of `/FileStore` or `/dbfs`.

### Issue 2 — Wrong File Path
**Problem:** Relative file paths caused path errors.

**Resolution:** Used absolute Unity Catalog path format:

```text
/Volumes/rohan_catalog/benefits_demo/input_files/
```

### Issue 3 — `input_file_name()` not supported
**Problem:** `input_file_name()` was not supported under Unity Catalog.

**Resolution:** Replaced with simpler source tracking logic using:

```python
lit("employee_batch_2.csv")
```

### Issue 4 — MERGE column mismatch
**Problem:** MERGE failed due to missing metadata columns.

**Resolution:** Added metadata columns to Bronze table:

```sql
ALTER TABLE rohan_catalog.benefits_demo.employee_data
ADD COLUMNS (
  LoadTimestamp TIMESTAMP,
  SourceFile STRING
);
```

### Issue 5 — Wrong temp view source
**Problem:** `updates` temp view was created from the wrong DataFrame.

**Resolution:** Ensured temp view was created only from the **final source DataFrame** after adding metadata.

---

## 16. Tools & Technologies Used

| Category | Tool / Technology |
|---|---|
| Platform | Databricks |
| Language | Python |
| Framework | PySpark |
| SQL Engine | Databricks SQL |
| Storage | Unity Catalog Volumes |
| Table Format | Delta Lake |
| Orchestration | Databricks Jobs |
| Architecture | Medallion Architecture |

---

## 17. Project Outcomes

At the end of this project, the following were successfully implemented:

- built an end-to-end batch pipeline in Databricks
- implemented Bronze, Silver, Gold architecture
- performed Delta MERGE-based upserts
- added metadata tracking
- automated workflow using Databricks Jobs
- generated reporting-ready summary output

---

## 18. Learning Outcomes

This project helped demonstrate practical understanding of:

- batch ingestion pipelines
- Delta Lake MERGE
- Databricks notebook orchestration
- Unity Catalog file handling
- Bronze / Silver / Gold architecture
- business rule transformation
- data aggregation for reporting

---

## 19. Interview Explanation (Short Version)

If an interviewer asks:

**“Tell me about your Databricks project”**

You can say:

> I built an end-to-end employee risk analytics pipeline in Databricks using Medallion Architecture. I ingested employee batch files into a Bronze Delta table using MERGE for upserts, transformed active employee data in the Silver layer by deriving a risk category, and aggregated the results into a Gold summary table for reporting. The entire workflow was automated using Databricks Jobs.

---

## 20. Future Enhancements

This project can be enhanced further by adding:

- Auto Loader for file automation
- schema validation
- data quality checks
- duplicate file detection
- CDC (Change Data Capture)
- SCD Type 2 history tracking
- Power BI dashboard integration

---

## 21. Final Conclusion

This project successfully demonstrates how to build a **real-world ETL pipeline in Databricks** using **Bronze → Silver → Gold architecture**.

It simulates a practical business use case from the **Benefits / HR Analytics domain**, while covering key Data Engineering concepts such as:

- batch ingestion
- upsert logic
- transformation rules
- aggregation
- orchestration
- metadata tracking

It is a strong beginner-to-intermediate **Data Engineering portfolio project** and also highly useful for **interview discussion**.
