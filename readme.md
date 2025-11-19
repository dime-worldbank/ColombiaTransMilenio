# Bogotá TuLlave Smartcard Data Analysis [TBC

## Data structure

**VOLUMES** 
* Ingestion + fetching
* Unique raw folder
* Folders by header

**DELTA TABLES**
* file_to_header:
* tm_bronze:
* bronze_raw_staging: auxiliar table to incrementally load files into tm_bronze using COPY INTO functionality.  


## Code structure 
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
     
 [TBC: organizing fromDocuments and fromData already decompresses everything]

#### 3. Classify data based on headers: `data-byheader`
  - Reorganize data files in folders by header 

#### 4. Clean data: `data-clean`
- Unify the structure across all datasets. Each header follows a specific format. Use different spark_handlers to import them and apply the appropriate transformations to each, then combine them into a single, unified dataset.
- Remove duplicates

### 5. Sampling: `data-sample`

### 6. Construction: 
1.  `constr-treatment-groups`
2.  `constr-monthly-panel-treatment`


### 6. Analysis:
- `plot`

### _TBC data before 2020_



 
## Questions to ask TM: 
- Some dates have "UTC" at the end of it and some others don't. Can we assume they are in UTC time as well? Or shall we assume that they are in Colombia time?
- Can the same card number, if not used for a while, be later assigned to another person