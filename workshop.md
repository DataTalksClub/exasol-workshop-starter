# Exasol Workshop

## Setting Up Exasol

### Install the Exasol CLI

The `exasol` CLI is pre-installed in the Codespace. If you're not using Codespaces, install it manually:

```bash
mkdir -p ~/bin
curl https://downloads.exasol.com/exasol-personal/installer.sh | bash
mv exasol ~/bin/
```

Or download it from the [Exasol Personal Edition page](https://downloads.exasol.com/exasol-personal) and place it in `~/bin/`.

### Deploy Exasol Personal Edition

Create a `deployment` directory inside the repo and run the installer:

```bash
mkdir deployment
cd deployment
exasol install
```

The `AWS_DEFAULT_REGION` is set to `eu-central-1` in the Codespace. If you're not using Codespaces, set it before running `exasol install`:

```bash
export AWS_DEFAULT_REGION=eu-central-1
```

Accept the EULA when prompted. The deployment takes 7-10 minutes.

All `exasol` commands must be run from within the deployment directory.

By default it deploys a single-node cluster with `r6i.xlarge` (4 vCPUs, 32 GB RAM). To customize:

```bash
exasol install --cluster-size 3 --instance-type r6i.2xlarge
```

Available instance types:

| Instance Type | vCPUs | RAM    | Use Case              |
|---------------|-------|--------|-----------------------|
| r6i.xlarge    | 4     | 32 GB  | Default, getting started |
| r6i.2xlarge   | 8     | 64 GB  | Larger workloads      |
| r6i.4xlarge   | 16    | 128 GB | High performance      |

The `exasol install` command generates Terraform files, provisions AWS infrastructure (VPC, EC2, security groups, etc.), and installs Exasol Personal on the EC2 instance.

If the deployment process is interrupted, EC2 instances may continue to accrue costs. Check the AWS console and manually terminate any orphaned instances.

When the deployment finishes, you will see connection details for the database, the Exasol Admin URL, SSH access information, and where to find passwords.

Make sure to add these to `.gitignore`:

```gitignore
deployment/secrets-*.json
deployment/*.pem
deployment/terraform.tfstate
deployment/.terraform/
deployment/tofu
deployment/.workflowState.json
```

- `secrets-*.json` - database and admin UI passwords
- `*.pem` - private SSH key for EC2 access
- `terraform.tfstate` - Terraform state with all resource details and secrets
- `.terraform/` - Terraform provider plugins (large)
- `tofu` - OpenTofu binary (~90 MB)
- `.workflowState.json` - internal workflow tracking

### Check the status

Once the deployment finishes, check that the database is running:

```bash
exasol status
```

You should see `database_ready` in the output.

### Get connection details

```bash
exasol info
```

This shows the host, port, and password for your Exasol instance. The connection details are also saved in:

- `deployment-exasol-<id>.json` - host, port, DNS name
- `secrets-exasol-<id>.json` - database and admin UI passwords

### Connect to the database

Use the built-in SQL client:

```bash
exasol connect
```

Try a simple query:

```sql
SELECT 'Hello, Exasol!' AS greeting;
```

Type `quit` or press `Ctrl+D` to exit.


## Loading NHS Prescription Data

We will load the [Prescribing by GP Practice](https://www.data.gov.uk/dataset/176ae264-2484-4afe-a297-d51798eb8228/prescribing-by-gp-practice-presentation-level) dataset published on data.gov.uk. This dataset contains monthly prescription records from GP practices across England from 2010 to 2018 - about 10 million rows per month, over 1 billion rows total.

Each month has 3 CSV files. In data warehousing terms, they form a star schema with one fact table and two dimension tables:

- Fact table (PDPI) - the main table with measurements (items prescribed, costs). It's large (~10M rows per month) and contains foreign keys to the dimension tables.
- Dimension tables (ADDR, CHEM) - smaller lookup tables that describe the entities referenced in the fact table. They provide context: where is the practice located (ADDR), what is the chemical name for a code (CHEM).

```mermaid
erDiagram
    PDPI ||--o{ ADDR : "PRACTICE"
    PDPI ||--o{ CHEM : "BNF_CODE (first 9 chars)"

    PDPI {
        varchar SHA
        varchar PCT
        varchar PRACTICE
        varchar BNF_CODE
        varchar BNF_NAME
        decimal ITEMS
        decimal NIC
        decimal ACT_COST
        decimal QUANTITY
        varchar PERIOD
    }

    ADDR {
        varchar PRACTICE_CODE
        varchar PRACTICE_NAME
        varchar ADDRESS_1
        varchar POSTCODE
    }

    CHEM {
        varchar CHEM_SUB
        varchar NAME
    }
```

Let's download October 2010 (the first available month) and explore each file:

```bash
mkdir data
```

### ADDR - practice addresses (dimension)

[ADDR](https://files.digital.nhs.uk/7D/F8A6AF/T201008ADDR%20BNFT.CSV) - ~10K rows per month:

```bash
wget "https://files.digital.nhs.uk/7D/F8A6AF/T201008ADDR%20BNFT.CSV" -O data/addr_201008.csv
```

Look at the first few rows:

```bash
head -5 data/addr_201008.csv
```

You'll see rows like:

```
201008,A81001,THE DENSHAM SURGERY                     ,THE HEALTH CENTRE        ,LAWSON STREET            ,STOCKTON                 ,CLEVELAND                ,TS18 1HU                 ,
```

The data is comma-separated but values are heavily padded with spaces, and there's a trailing comma at the end creating an extra empty column. There's no header row - data starts on the first line.

```bash
wc -l data/addr_201008.csv
```

About 10,263 lines.

```bash
file data/addr_201008.csv
```

Output:

```
data/addr_201008.csv: CSV ASCII text
```

No mention of CRLF, so this file uses LF (`\n`) line endings.

The columns are: PERIOD, PRACTICE_CODE, PRACTICE_NAME, ADDRESS_1, ADDRESS_2, ADDRESS_3, COUNTY, POSTCODE.

Our goal is to get a clean table like this:

| PERIOD | PRACTICE_CODE | PRACTICE_NAME | ADDRESS_1 | ADDRESS_2 | ADDRESS_3 | COUNTY | POSTCODE |
|--------|---------------|---------------|-----------|-----------|-----------|--------|----------|
| 201008 | A81001 | THE DENSHAM SURGERY | THE HEALTH CENTRE | LAWSON STREET | STOCKTON | CLEVELAND | TS18 1HU |
| 201008 | A81002 | QUEENS PARK MEDICAL CENTRE | QUEENS PARK MEDICAL CTR | FARRER STREET | STOCKTON ON TEES | CLEVELAND | TS18 2AW |

- Each row is a GP practice with its address and postcode
- The PRACTICE_CODE is the key that links to the PRACTICE field in PDPI, so we can join them to answer geographic questions (e.g. prescriptions in a specific postcode area)
- To get there we need to TRIM the space-padded values and drop the extra empty column

### Manual loading

Let's load this first month manually with SQL to understand the process, then we'll automate it with Python.

Connect to the database and create a schema:

```bash
exasol connect
```

Create a staging schema. We call it "staging" because this is where we load the raw data before cleaning it up and moving it to the final tables:

```sql
CREATE SCHEMA IF NOT EXISTS PRESCRIPTIONS_UK_STAGING;
OPEN SCHEMA PRESCRIPTIONS_UK_STAGING;
```

First, we need a table to hold the raw data. The column definitions must match the CSV exactly - including the extra empty column from the trailing comma. We use wide VARCHARs because the values are space-padded, and if the column is too narrow, the database will reject the import:

```sql
CREATE TABLE STG_RAW_ADDR_201008 (
    PERIOD VARCHAR(100),
    PRACTICE_CODE VARCHAR(100),
    PRACTICE_NAME VARCHAR(2000),
    ADDRESS_1 VARCHAR(2000),
    ADDRESS_2 VARCHAR(2000),
    ADDRESS_3 VARCHAR(2000),
    COUNTY VARCHAR(2000),
    POSTCODE VARCHAR(200),
    EXTRA_PADDING VARCHAR(2000)
);
```

The `exasol connect` terminal treats newlines as Enter, so multi-line SQL doesn't paste well. Here's the same statement as a single line you can copy-paste into the terminal (later we'll switch to Python where this won't be an issue):

```sql
CREATE TABLE STG_RAW_ADDR_201008 (PERIOD VARCHAR(100), PRACTICE_CODE VARCHAR(100), PRACTICE_NAME VARCHAR(2000), ADDRESS_1 VARCHAR(2000), ADDRESS_2 VARCHAR(2000), ADDRESS_3 VARCHAR(2000), COUNTY VARCHAR(2000), POSTCODE VARCHAR(200), EXTRA_PADDING VARCHAR(2000));
```

Now load the data. Exasol's `IMPORT FROM CSV AT` can fetch CSV files directly from HTTP URLs. The URL is split into a base (`AT`) and filename (`FILE`). We set the format based on what we found earlier - LF line endings and no header row (SKIP = 0):

```sql
IMPORT INTO STG_RAW_ADDR_201008
FROM CSV AT 'https://files.digital.nhs.uk/7D/F8A6AF'
FILE 'T201008ADDR%20BNFT.CSV'
COLUMN SEPARATOR = ','
ROW SEPARATOR = 'LF'
SKIP = 0
ENCODING = 'UTF8';
```

Single line:

```sql
IMPORT INTO STG_RAW_ADDR_201008 FROM CSV AT 'https://files.digital.nhs.uk/7D/F8A6AF' FILE 'T201008ADDR%20BNFT.CSV' COLUMN SEPARATOR = ',' ROW SEPARATOR = 'LF' SKIP = 0 ENCODING = 'UTF8';
```

Check how many rows were loaded:

```sql
SELECT COUNT(*) FROM STG_RAW_ADDR_201008;
```

Check a few rows:

```sql
SELECT * FROM STG_RAW_ADDR_201008 LIMIT 5;
```

Result: 

```
> SELECT * FROM STG_RAW_ADDR_201008 LIMIT 5;
┌──────┬────────────┬─────────────┬──────────┬──────────┬──────────┬───────┬─────────┬─────────────┐
│ PERI │ PRACTICE_C │ PRACTICE_NA │ ADDRESS_ │ ADDRESS_ │ ADDRESS_ │ COUNT │ POSTCOD │ EXTRA_PADDI │
├──────┼────────────┼─────────────┼──────────┼──────────┼──────────┼───────┼─────────┼─────────────┤
│ 2010 │ F84744     │ WHITECHAPEL │ 174 WHIT │          │ LONDON   │       │ E1 1BZ  │             │
│ 2010 │ M91660     │ DARLASTON H │ PINFOLD  │ DARLASTO │          │       │ WS10 8S │             │
│ 2010 │ G81696     │ THE CHASELE │ GREEN ST │ 118-122  │ EASTBOUR │       │ BN21 1R │             │
│ 2010 │ P87663     │ SWINTON HAL │ THE COTT │ SWINTON  │ 188 WORS │ SWINT │ M27 5SN │             │
│ 2010 │ H82645     │ CRAWLEY DAY │ 1ST FLOO │ BROADFIE │ BROADFIE │ WEST  │ RH11 9B │             │
└──────┴────────────┴─────────────┴──────────┴──────────┴──────────┴───────┴─────────┴─────────────┘
```

The terminal truncates the columns, so it's not obvious here, but the values are still heavily padded with spaces (as we saw in the raw CSV). We want to TRIM that padding and drop the useless EXTRA_PADDING column. This is the next step - moving the data from the raw table to a clean staging table:

```sql
CREATE TABLE STG_ADDR_201008 (
    PERIOD VARCHAR(6),
    PRACTICE_CODE VARCHAR(20),
    PRACTICE_NAME VARCHAR(200),
    ADDRESS_1 VARCHAR(200),
    ADDRESS_2 VARCHAR(200),
    ADDRESS_3 VARCHAR(200),
    COUNTY VARCHAR(200),
    POSTCODE VARCHAR(20)
);
```

Single line:

```sql
CREATE TABLE STG_ADDR_201008 (PERIOD VARCHAR(6), PRACTICE_CODE VARCHAR(20), PRACTICE_NAME VARCHAR(200), ADDRESS_1 VARCHAR(200), ADDRESS_2 VARCHAR(200), ADDRESS_3 VARCHAR(200), COUNTY VARCHAR(200), POSTCODE VARCHAR(20));
```

Insert with TRIM to strip the padding:

```sql
INSERT INTO STG_ADDR_201008
SELECT '201008', TRIM(PRACTICE_CODE), TRIM(PRACTICE_NAME),
       TRIM(ADDRESS_1), TRIM(ADDRESS_2), TRIM(ADDRESS_3),
       TRIM(COUNTY), TRIM(POSTCODE)
FROM STG_RAW_ADDR_201008;
```

Single line:

```sql
INSERT INTO STG_ADDR_201008 SELECT '201008', TRIM(PRACTICE_CODE), TRIM(PRACTICE_NAME), TRIM(ADDRESS_1), TRIM(ADDRESS_2), TRIM(ADDRESS_3), TRIM(COUNTY), TRIM(POSTCODE) FROM STG_RAW_ADDR_201008;
```

To verify the padding is gone, compare the string lengths before and after:

```sql
SELECT LENGTH(r.PRACTICE_NAME) AS raw_len, LENGTH(s.PRACTICE_NAME) AS clean_len, s.PRACTICE_NAME FROM STG_RAW_ADDR_201008 r JOIN STG_ADDR_201008 s ON TRIM(r.PRACTICE_CODE) = s.PRACTICE_CODE LIMIT 5;
```

You should see that `raw_len` is much larger than `clean_len` (e.g. 40 vs 19) - that's all the space padding we removed.

Drop the raw table:

```sql
DROP TABLE STG_RAW_ADDR_201008;
```

Verify the clean data:

```sql
SELECT * FROM STG_ADDR_201008 LIMIT 5;
```

### CHEM - chemical substances (dimension)

[CHEM](https://files.digital.nhs.uk/15/ED9D38/T201008CHEM%20SUBS.CSV) - ~3.5K rows per month:

```bash
wget "https://files.digital.nhs.uk/15/ED9D38/T201008CHEM%20SUBS.CSV" -O data/chem_201008.csv
```

Look at the first few rows:

```bash
head -5 data/chem_201008.csv
```

You'll see:

```
CHEM SUB ,NAME,                                                       201008,
0101010A0,Alexitol Sodium                                             ,
0101010B0,Almasilate                                                  ,
0101010C0,Aluminium Hydroxide                                         ,
0101010D0,Aluminium Hydroxide With Magnesium                          ,
```

Compare this with ADDR:

```bash
wc -l data/chem_201008.csv
```

About 3,290 lines. Check the line endings:

```bash
file data/chem_201008.csv
```

Output:

```
data/chem_201008.csv: ASCII text, with CRLF line terminators
```

Comparing with ADDR:

- ADDR had no header row at all, while CHEM has a header - but it's unusual: the third column contains the period value `201008` instead of a column name
- The data rows only have 2 values (code and name) plus a trailing comma
- Same space-padding as ADDR
- ADDR used LF line endings, but CHEM uses CRLF - so we'll need a different `ROW SEPARATOR` when loading

### Loading CHEM into Exasol

CRLF line endings, has header (SKIP = 1), 3 columns:

```sql
CREATE TABLE STG_RAW_CHEM_201008 (
    CHEM_SUB VARCHAR(50),
    NAME VARCHAR(2000),
    PERIOD VARCHAR(200)
);
```

Single line:

```sql
CREATE TABLE STG_RAW_CHEM_201008 (CHEM_SUB VARCHAR(50), NAME VARCHAR(2000), PERIOD VARCHAR(200));
```

Import the data:

```sql
IMPORT INTO STG_RAW_CHEM_201008
FROM CSV AT 'https://files.digital.nhs.uk/15/ED9D38'
FILE 'T201008CHEM%20SUBS.CSV'
COLUMN SEPARATOR = ','
ROW SEPARATOR = 'CRLF'
SKIP = 1
ENCODING = 'UTF8';
```

Single line:

```sql
IMPORT INTO STG_RAW_CHEM_201008 FROM CSV AT 'https://files.digital.nhs.uk/15/ED9D38' FILE 'T201008CHEM%20SUBS.CSV' COLUMN SEPARATOR = ',' ROW SEPARATOR = 'CRLF' SKIP = 1 ENCODING = 'UTF8';
```

```sql
SELECT COUNT(*) FROM STG_RAW_CHEM_201008;
```

Check a few rows:

```sql
SELECT * FROM STG_RAW_CHEM_201008 LIMIT 5;
```

Clean up with TRIM:

```sql
CREATE TABLE STG_CHEM_201008 (
    CHEM_SUB VARCHAR(15),
    NAME VARCHAR(200),
    PERIOD VARCHAR(6)
);
```

Single line:

```sql
CREATE TABLE STG_CHEM_201008 (CHEM_SUB VARCHAR(15), NAME VARCHAR(200), PERIOD VARCHAR(6));
```

Insert with TRIM:

```sql
INSERT INTO STG_CHEM_201008
SELECT TRIM(CHEM_SUB), TRIM(NAME), '201008'
FROM STG_RAW_CHEM_201008;
```

Single line:

```sql
INSERT INTO STG_CHEM_201008 SELECT TRIM(CHEM_SUB), TRIM(NAME), '201008' FROM STG_RAW_CHEM_201008;
```

Drop the raw table:

```sql
DROP TABLE STG_RAW_CHEM_201008;
```

Verify the clean data:

```sql
SELECT * FROM STG_CHEM_201008 LIMIT 5;
```

### PDPI - prescriptions (fact)

[PDPI](https://files.digital.nhs.uk/B9/14BEAF/T201008PDPI%20BNFT.CSV) - ~10M rows per month.

The full file is over 1 GB, so we use `curl -r` to download just the first 10KB:

```bash
curl -r 0-9999 "https://files.digital.nhs.uk/B9/14BEAF/T201008PDPI%20BNFT.CSV" -o data/pdpi_201008_sample.csv
```

Look at the first few rows:

```bash
head -5 data/pdpi_201008_sample.csv
```

You'll see:

```
 SHA,PCT,PRACTICE,BNF CODE,BNF NAME                                    ,ITEMS  ,NIC        ,ACT COST   ,QUANTITY,PERIOD,
Q30,5D7,A86003,0101010G0AAABAB,Co-Magaldrox_Susp 195mg/220mg/5ml S/F   ,0000031,00000083.79,00000078.12,0018500,201008,
Q30,5D7,A86003,0101010J0AAAAAA,Mag Trisil_Mix                          ,0000002,00000011.28,00000010.44,0002400,201008,
Q30,5D7,A86003,0101010P0AAAAAA,Co-Simalcite_Susp 125mg/500mg/5ml S/F   ,0000002,00000009.89,00000009.17,0001000,201008,
```

This is the largest file - the fact table with all prescription records.

Comparing with ADDR and CHEM:

- Has a header row (like CHEM, unlike ADDR)
- Same space-padding and trailing comma as the other files
- Additionally, numeric columns are zero-padded (e.g. `0000031`, `00000083.79`) - the other files didn't have this
- PRACTICE column links to PRACTICE_CODE in ADDR
- The first 9 characters of BNF CODE correspond to CHEM SUB in CHEM

Check the line endings:

```bash
file data/pdpi_201008_sample.csv
```

Output:

```
data/pdpi_201008_sample.csv: CSV ASCII text
```

LF line endings, same as ADDR.

- Each row is one prescription: which practice prescribed what drug (BNF CODE/NAME), how many items, the cost (NIC = net ingredient cost, ACT COST = actual cost), and the quantity dispensed
- PRACTICE links to ADDR, the first 9 characters of BNF CODE link to CHEM SUB
- Values are padded with spaces and numbers are zero-padded (e.g. `0000031`, `00000083.79`) - Exasol handles zero-padding automatically when importing into DECIMAL columns
- There's a trailing comma after the last field, creating an extra empty column
- The file has a header row

### Loading PDPI into Exasol

LF line endings, has header (SKIP = 1), 11 columns (including the trailing empty one). This one takes a minute or two since it's loading ~10M rows over the network:

```sql
CREATE TABLE STG_RAW_PDPI_201008 (
    SHA VARCHAR(100),
    PCT VARCHAR(100),
    PRACTICE VARCHAR(100),
    BNF_CODE VARCHAR(50),
    BNF_NAME VARCHAR(2000),
    ITEMS DECIMAL(18,0),
    NIC DECIMAL(18,2),
    ACT_COST DECIMAL(18,2),
    QUANTITY DECIMAL(18,0),
    PERIOD VARCHAR(100),
    EXTRA_PADDING VARCHAR(2000)
);
```

Single line:

```sql
CREATE TABLE STG_RAW_PDPI_201008 (SHA VARCHAR(100), PCT VARCHAR(100), PRACTICE VARCHAR(100), BNF_CODE VARCHAR(50), BNF_NAME VARCHAR(2000), ITEMS DECIMAL(18,0), NIC DECIMAL(18,2), ACT_COST DECIMAL(18,2), QUANTITY DECIMAL(18,0), PERIOD VARCHAR(100), EXTRA_PADDING VARCHAR(2000));
```

Import the data - this takes a minute or two:

```sql
IMPORT INTO STG_RAW_PDPI_201008
FROM CSV AT 'https://files.digital.nhs.uk/B9/14BEAF'
FILE 'T201008PDPI%20BNFT.CSV'
COLUMN SEPARATOR = ','
ROW SEPARATOR = 'LF'
SKIP = 1
ENCODING = 'UTF8';
```

Single line:

```sql
IMPORT INTO STG_RAW_PDPI_201008 FROM CSV AT 'https://files.digital.nhs.uk/B9/14BEAF' FILE 'T201008PDPI%20BNFT.CSV' COLUMN SEPARATOR = ',' ROW SEPARATOR = 'LF' SKIP = 1 ENCODING = 'UTF8';
```

Check how many rows were loaded:

```sql
SELECT COUNT(*) FROM STG_RAW_PDPI_201008;
```

If the result shows scientific notation (e.g. `9.799052e+06`), use `TO_CHAR` to see the actual number:

```sql
SELECT TO_CHAR(COUNT(*)) FROM STG_RAW_PDPI_201008;
```

Check a few rows:

```sql
SELECT * FROM STG_RAW_PDPI_201008 LIMIT 5;
```

Clean up with TRIM:

```sql
CREATE TABLE STG_PDPI_201008 (
    SHA VARCHAR(10),
    PCT VARCHAR(10),
    PRACTICE VARCHAR(20),
    BNF_CODE VARCHAR(15),
    BNF_NAME VARCHAR(200),
    ITEMS DECIMAL(18,0),
    NIC DECIMAL(18,2),
    ACT_COST DECIMAL(18,2),
    QUANTITY DECIMAL(18,0),
    PERIOD VARCHAR(6)
);
```

Single line:

```sql
CREATE TABLE STG_PDPI_201008 (SHA VARCHAR(10), PCT VARCHAR(10), PRACTICE VARCHAR(20), BNF_CODE VARCHAR(15), BNF_NAME VARCHAR(200), ITEMS DECIMAL(18,0), NIC DECIMAL(18,2), ACT_COST DECIMAL(18,2), QUANTITY DECIMAL(18,0), PERIOD VARCHAR(6));
```

Insert with TRIM:

```sql
INSERT INTO STG_PDPI_201008
SELECT TRIM(SHA), TRIM(PCT), TRIM(PRACTICE), TRIM(BNF_CODE), TRIM(BNF_NAME),
       ITEMS, NIC, ACT_COST, QUANTITY, '201008'
FROM STG_RAW_PDPI_201008;
```

Single line:

```sql
INSERT INTO STG_PDPI_201008 SELECT TRIM(SHA), TRIM(PCT), TRIM(PRACTICE), TRIM(BNF_CODE), TRIM(BNF_NAME), ITEMS, NIC, ACT_COST, QUANTITY, '201008' FROM STG_RAW_PDPI_201008;
```

Drop the raw table:

```sql
DROP TABLE STG_RAW_PDPI_201008;
```

Verify the clean data:

```sql
SELECT * FROM STG_PDPI_201008 LIMIT 5;
```

## Manual data warehouse load

Now that we have clean staging tables, let's create the final data warehouse tables in a separate schema. We use `PRESCRIPTIONS_UK` for the final tables - keeping them separate from `PRESCRIPTIONS_UK_STAGING` so analysts get a clean schema with only the tables they need:

```sql
CREATE SCHEMA IF NOT EXISTS PRESCRIPTIONS_UK;
```

The PRACTICE dimension table, built from the ADDR staging data:

```sql
CREATE TABLE PRESCRIPTIONS_UK.PRACTICE (
    PRACTICE_CODE VARCHAR(20),
    PRACTICE_NAME VARCHAR(200),
    ADDRESS_1 VARCHAR(200),
    ADDRESS_2 VARCHAR(200),
    ADDRESS_3 VARCHAR(200),
    COUNTY VARCHAR(200),
    POSTCODE VARCHAR(20)
);
```

Single line:

```sql
CREATE TABLE PRESCRIPTIONS_UK.PRACTICE (PRACTICE_CODE VARCHAR(20), PRACTICE_NAME VARCHAR(200), ADDRESS_1 VARCHAR(200), ADDRESS_2 VARCHAR(200), ADDRESS_3 VARCHAR(200), COUNTY VARCHAR(200), POSTCODE VARCHAR(20));
```

Populate it from the staging table:

```sql
INSERT INTO PRESCRIPTIONS_UK.PRACTICE
SELECT PRACTICE_CODE, PRACTICE_NAME, ADDRESS_1, ADDRESS_2, ADDRESS_3, COUNTY, POSTCODE
FROM PRESCRIPTIONS_UK_STAGING.STG_ADDR_201008;
```

Single line:

```sql
INSERT INTO PRESCRIPTIONS_UK.PRACTICE SELECT PRACTICE_CODE, PRACTICE_NAME, ADDRESS_1, ADDRESS_2, ADDRESS_3, COUNTY, POSTCODE FROM PRESCRIPTIONS_UK_STAGING.STG_ADDR_201008;
```

The CHEMICAL dimension table, built from the CHEM staging data:

```sql
CREATE TABLE PRESCRIPTIONS_UK.CHEMICAL (
    CHEMICAL_CODE VARCHAR(15),
    CHEMICAL_NAME VARCHAR(200)
);
```

Single line:

```sql
CREATE TABLE PRESCRIPTIONS_UK.CHEMICAL (CHEMICAL_CODE VARCHAR(15), CHEMICAL_NAME VARCHAR(200));
```

Populate it:

```sql
INSERT INTO PRESCRIPTIONS_UK.CHEMICAL
SELECT CHEM_SUB, NAME
FROM PRESCRIPTIONS_UK_STAGING.STG_CHEM_201008;
```

Single line:

```sql
INSERT INTO PRESCRIPTIONS_UK.CHEMICAL SELECT CHEM_SUB, NAME FROM PRESCRIPTIONS_UK_STAGING.STG_CHEM_201008;
```

The PRESCRIPTION fact table with the actual prescription records:

```sql
CREATE TABLE PRESCRIPTIONS_UK.PRESCRIPTION (
    SHA VARCHAR(10),
    PCT VARCHAR(10),
    PRACTICE_CODE VARCHAR(20),
    BNF_CODE VARCHAR(15),
    DRUG_NAME VARCHAR(200),
    ITEMS DECIMAL(18,0),
    NET_COST DECIMAL(18,2),
    ACTUAL_COST DECIMAL(18,2),
    QUANTITY DECIMAL(18,0),
    PERIOD VARCHAR(6)
);
```

Single line:

```sql
CREATE TABLE PRESCRIPTIONS_UK.PRESCRIPTION (SHA VARCHAR(10), PCT VARCHAR(10), PRACTICE_CODE VARCHAR(20), BNF_CODE VARCHAR(15), DRUG_NAME VARCHAR(200), ITEMS DECIMAL(18,0), NET_COST DECIMAL(18,2), ACTUAL_COST DECIMAL(18,2), QUANTITY DECIMAL(18,0), PERIOD VARCHAR(6));
```

Populate it from the PDPI staging table:

```sql
INSERT INTO PRESCRIPTIONS_UK.PRESCRIPTION
SELECT SHA, PCT, PRACTICE, BNF_CODE, BNF_NAME, ITEMS, NIC, ACT_COST, QUANTITY, PERIOD
FROM PRESCRIPTIONS_UK_STAGING.STG_PDPI_201008;
```

Single line:

```sql
INSERT INTO PRESCRIPTIONS_UK.PRESCRIPTION SELECT SHA, PCT, PRACTICE, BNF_CODE, BNF_NAME, ITEMS, NIC, ACT_COST, QUANTITY, PERIOD FROM PRESCRIPTIONS_UK_STAGING.STG_PDPI_201008;
```

Now let's switch to the final schema and verify the row counts:

```sql
OPEN SCHEMA PRESCRIPTIONS_UK;
```

Check each table:

```sql
SELECT TO_CHAR(COUNT(*)) AS practices FROM PRACTICE;
SELECT TO_CHAR(COUNT(*)) AS chemicals FROM CHEMICAL;
SELECT TO_CHAR(COUNT(*)) AS prescriptions FROM PRESCRIPTION;
```

Try a query that joins all three tables - find the top 5 most prescribed chemicals:

```sql
SELECT c.CHEMICAL_NAME, SUM(rx.ITEMS) AS total_items
FROM PRESCRIPTION rx
JOIN CHEMICAL c ON SUBSTR(rx.BNF_CODE, 1, 9) = c.CHEMICAL_CODE
GROUP BY c.CHEMICAL_NAME
ORDER BY total_items DESC
LIMIT 5;
```

Single line:

```sql
SELECT c.CHEMICAL_NAME, SUM(rx.ITEMS) AS total_items FROM PRESCRIPTION rx JOIN CHEMICAL c ON SUBSTR(rx.BNF_CODE, 1, 9) = c.CHEMICAL_CODE GROUP BY c.CHEMICAL_NAME ORDER BY total_items DESC LIMIT 5;
```

Exit the SQL client:

```sql
quit
```

### Format differences between months

Some months use CRLF (`\r\n`) line endings, others use LF (`\n`) - this affects the `ROW SEPARATOR` in the IMPORT statement. You can check with `file`:

```bash
file data/pdpi_201008_sample.csv
```

This will show `CRLF line terminators` or just `ASCII text` (which means LF). The number of columns can also vary between months (some have extra padding columns).

The `IMPORT` statement needs the row separator (`CRLF` or `LF`) and whether to skip a header row. These settings vary between files - some months use Windows-style line endings (`CRLF`), others use Unix (`LF`), and some have extra padding columns. The `detect_format.py` script figures this out by downloading just the first 4KB of a file using an HTTP Range request, then checking for `\r\n` vs `\n` to determine the row separator, counting commas to get the number of columns, and looking for known header names (like `SHA`, `PRACTICE`, `BNF CODE`) to detect whether the first row is a header:

```bash
cd ../code
wget https://raw.githubusercontent.com/alexeygrigorev/exasol-workshop-starter/main/code/detect_format.py
uv run python detect_format.py --period 201008
```

Each month also has ADDR (practice addresses) and CHEM (chemical names) files that get loaded the same way. The Python script below automates this for all months, detects format differences, and trims whitespace.

### Find available data URLs

```bash
cd code
uv init
uv add requests beautifulsoup4
```

Download the script and run it:

```bash
wget https://raw.githubusercontent.com/alexeygrigorev/exasol-workshop-starter/main/code/find_urls.py
uv run python find_urls.py
```

This scrapes the [dataset page](https://www.data.gov.uk/dataset/176ae264-2484-4afe-a297-d51798eb8228/prescribing-by-gp-practice-presentation-level) and saves `data/prescription_urls.json` with ~101 months of data (2010-2018).

### Download the ingestion script

```bash
uv add pyexasol
wget https://raw.githubusercontent.com/alexeygrigorev/exasol-workshop-starter/main/code/ingest.py
```

### Stage data

Load one month to test:

```bash
uv run python ingest.py stage --period 201506
```

Load a full year:

```bash
uv run python ingest.py stage --year 2015
```

Load everything (~101 months):

```bash
uv run python ingest.py stage --all
```

Already-loaded months are skipped automatically. Use `--force` to reload.

### Create final tables

Once staging is done, create the final PRESCRIPTIONS, PRACTICE, and CHEMICAL tables:

```bash
uv run python ingest.py finalize
```

### Clean up staging tables

```bash
uv run python ingest.py cleanup
```

### Check what's loaded

```bash
uv run python ingest.py summary
```

### Run the challenge queries

```bash
uv run python ingest.py query
```

This answers two questions about East Central London (EC postcodes):
1. Top 3 most prescribed chemicals
2. The year with the most prescriptions of the top chemical


## Managing the cluster

### Stopping and resuming

Stop the instance when you're not using it (to save costs):

```bash
exasol stop
```

Resume later:

```bash
exasol start
```

Note that the IPs change after restart

### Destroying the deployment

When you're completely done with the workshop:

```bash
exasol destroy
```

This terminates the EC2 instance and cleans up all AWS resources.


## Troubleshooting

- Codespace created before setting the secret? Rebuild it: `Cmd/Ctrl+Shift+P` -> "Rebuild Container"
- "Wrong passphrase"? Double-check with your instructor
- Permission errors on AWS? Ask your instructor -- the role may need updated permissions
- `exasol install` fails? Make sure `aws sts get-caller-identity` works first
- Lock file error? Remove `~/deployment/.exasolLock.json` and retry

