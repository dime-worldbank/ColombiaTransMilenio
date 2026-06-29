# Bogotá TuLlave Smartcard Data Analysis [TBC

## Data structure

### VOLUMES
'classical' storage of files in multiple formats
* Here is where data is stored in the first place
* Data moves through 3 stages 
  * Original entry points: 
      * Ingestion point (`/Documents`, for data we had saved in OneDrive before)
      * Fetching point (`/Data`, for data we periodically download from TM public API)
  * `/Workspace/Raw` that reorganizes data in folders that make sense based on the data structure
    * `/Recharges`
    * `/from2016to2019`: **validaciones**, where **cards are numeric**, and different types of validaciones are mixed 
      * Data here comes from /Documents
      * Data here is in loose files *except for: 2019ER10074 folder with sip files, 30 folders for June 2018 data like VALTRONCAL_01-06-2018 with gz files*
    * `/since2020`: **validaciones**, where  **cards are alphanumeric**, and we have separate foders for each type of validaciones
      * Data here comes from /Documents (moved once) and from /Data (moved weekly)
      * Data here is organized in subfolders: `/ValidacionCable`, `/ValidacionDual`, `/ValidacionTroncal`, `/ValidacionZonal`, each has either csv or zip files
    * [⚠️ TO REMOVE!] `/byheader_dir`: **validaciones**, where files are in folders based on their header (columns/variables) to facilitate import of multiple files together


### DELTA TABLES
Storage in a more reliable and efficient way, _built on top of Parquet files stored in cloud storage (Azure Data Lake), with an additional transaction log that tracks all changes_.
* file_to_header:
* tm_bronze_20: importing all raw data
* bronze_raw_staging: auxiliar table to incrementally load files into tm_bronze using COPY INTO functionality.  


## Code structure 

Order

### Since 2020 Workflow
1. `data-fetch` (runs periodically on Mondays)
2. `data-organize-fromDocuments` (ran once - no need to run again)
3. `data-organize-fromData`  (runs periodically on Mondays)
3. `data-byheader-since2020` (runs periodically on Mondays)

#### 1. Download newest data: `data-fetch`
  - From TransMilenio GCloud API
  - Job that automatically runs all Mondays

#### 2. Put together old and new data: `data-organize`

- `data-organize-fromDocuments`:
    - Creates a Workspace/Raw folder and moves data from the ingestion Point (Documents folder)
    - We do this just once (after uploading all our data to MEGA)
  
  
 -  `data-organize-fromData`: 
    - Moves data to the Workspace/Raw from the downloads Point (Data folder)
    - We do this periodically (after fetching a new batch of data every week) 
    - Job that automatically runs all Mondays, if `data-fetch` succeeds
     
####  NEW WORKFLOW 
3. Scan raw files to detect their header and record their mapping: `data-byheader-since2020`
4. Ingest data into the bronze table based on its header:  `data-byheader-since2020`
5. ⬜ `data-ingest-bronze-since2020` (run periodically)
6. ⬜ `data-clean-silver-since2020` (run periodically)
7. ⬜ gold table, samples and files to export

####  OLD WORKFLOW
Before, what we did agter organizing files was
3. `data-byheader` copied files to `/byheader_dir` (innecesary duplication)
4. `data-clean`
  - Unify the structure across all datasets. Each header follows a specific format. Use different spark_handlers to import them and apply the appropriate transformations to each, then combine them into a single, unified dataset.
  - Remove duplicates
  - Saved .... [complete]
5.  `data-sample`
  - [complete what it did]
  - Saved .... [complete]
6.  `constr-treatment-groups`
  - [complete what it did]
  - Saved .... [complete]
7.  `constr-monthly-panel-treatment`
  - [complete what it did]
  - Saved .... [complete]
8.  `plot`
  - [complete what it did]
  - Saved .... [complete]


### from2016to2019 Workflow
1. `data-organize-fromDocuments` (ran once - no need to run again)
2. `data-byheader-from2016to2019` (to be done and run once)
3. ⬜  `data-ingest-bronze-since2020` (run periodically)
4. ⬜  `data-clean-silver-since2020` (run periodically)
5. ⬜ gold table, samples and files to export


 
## Questions to ask TM: 
- Some dates have "UTC" at the end of it and some others don't. Can we assume they are in UTC time as well? Or shall we assume that they are in Colombia time?
- Can the same card number, if not used for a while, be later assigned to another person