# Bogotá TuLlave Smartcard Data Analysis

## Files structure

## Code structure
#### 1. Download newest data: `data-fetch`
  - From TransMilenio GCloud API
  - Job that automatically runs all Mondays
#### 2. Put together old and new data: `data-organize`
  - Create a Workspace folder and move data from:
    - Ingestion Point (Documents folder)
    - Downloads Point (Data folder)
  - _TBC data before 2020_
#### 3. Clean data: `data-clean`
- Uniform structure for all data
  1. Classify raw data files based on **headers**, and reorganize them in folders by header.
  2. Each header has a specific format. Import them with different spark_handlers to apply the right transformations to each.

 - _TBC data before 2020_



 
## Questions to ask TM: 
- Some dates have "UTC" at the end of it and some others don't. Can we assume they are in UTC time as well? Or shall we assume that they are in Colombia time?
- Can the same card number, if not used for a while, be later assigned to another person