# Databricks notebook source
# MAGIC %md
# MAGIC # Classify files by header (for data from 2016 to 2019)
# MAGIC Classifies raw CSV/zip files into header groups, detects per-file import parameters (encoding, delimiter, archive format), and records results in the file_classification_since2020 Delta table.
# MAGIC No physical file copying — files stay in their original location.

# COMMAND ----------

# DBTITLE 1,Install Packages
# Pip install non-standard packages
%pip install tqdm chardet openpyxl xlrd

import os
import csv
import zipfile
from tqdm import tqdm
import pandas as pd
from random import seed
import chardet
from collections import defaultdict

# COMMAND ----------

# Additional imports (some overlap with cell 2 for clarity)
import numpy as np
import pandas as pd


# COMMAND ----------

# MAGIC %run ./utils/handle_files

# COMMAND ----------

# DBTITLE 1,Set Directories
# Directories
S_DIR = '/Volumes/prd_csc_mega/sColom15/'
V_DIR = f'{S_DIR}vColom15/'
user = 'wbrau@worldbank.org'
git = f'/Workspace/Users/{user}/ColombiaTransMilenio'

# Important sub-directories for this notebook
raw_dir = V_DIR + '/Workspace/Raw/'

# COMMAND ----------

# DBTITLE 1,Set default catalog and schema
# MAGIC %sql
# MAGIC
# MAGIC USE CATALOG prd_mega;
# MAGIC USE SCHEMA scolom15;
# MAGIC
# MAGIC SELECT
# MAGIC   current_catalog() as current_catalog,
# MAGIC   current_schema()  as current_schema;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Create Table with Needed Columns
# MAGIC %sql
# MAGIC -- ─────────────────────────────────────────────────────────────────────────────
# MAGIC -- CREATE THE CLASSIFICATION TABLE
# MAGIC -- Store one row per raw file, recording:
# MAGIC --   • which header group it belongs to (e.g. header_one, header_17)
# MAGIC --   • how to read it (encoding, delimiter, archive format)
# MAGIC --   • its classification status (ready / needs_review / broken)
# MAGIC --
# MAGIC -- IF NOT EXISTS means this is safe to re-run — it won't overwrite an
# MAGIC -- existing table. If you need to reset, use the DROP TABLE cell above.
# MAGIC -- ─────────────────────────────────────────────────────────────────────────────
# MAGIC CREATE TABLE IF NOT EXISTS file_classification_from2016to2019 (
# MAGIC     raw_filepath          STRING COMMENT 'Full path to the raw source file',
# MAGIC     header                STRING COMMENT 'Header group assigned after parsing and normalizing the file header row',
# MAGIC     source_period         STRING COMMENT 'Source period label for this batch of files',
# MAGIC     zipped                INT COMMENT '1 when the file was detected as a zip archive during classification, 0 otherwise',
# MAGIC     file_format           STRING COMMENT 'Underlying file format used after archive handling, expected csv for this dataset',
# MAGIC     archive_format        STRING COMMENT 'Archive format detected from the file bytes, for example zip',
# MAGIC     delimiter             STRING COMMENT 'Delimiter that produced the matched normalized header',
# MAGIC     encoding              STRING COMMENT 'Encoding actually used to decode the header row',
# MAGIC     encoding_source       STRING COMMENT 'How the encoding value was obtained, for example chardet_file, chardet_zip_member, fallback_latin-1, pandas_fallback, or known_broken_list',
# MAGIC     transform_format      STRING COMMENT 'Downstream transform mapping implied by the detected header',
# MAGIC     inner_file_name       STRING COMMENT 'Inner member name used when the source file is a zip archive',
# MAGIC     header_row            INT COMMENT '1-indexed row number used as the header during classification',
# MAGIC     normalized_header     STRING COMMENT 'Normalized header row stored as a JSON array string',
# MAGIC     classification_status STRING COMMENT 'Classification outcome, for example ready, unsupported_empty_header, broken, or needs_review',
# MAGIC     detection_notes       STRING COMMENT 'Notes about fallbacks, parser behavior, or detection issues observed during classification',
# MAGIC     detected_at           TIMESTAMP COMMENT 'Timestamp when the file classification metadata was generated',
# MAGIC     ingested_at           TIMESTAMP COMMENT 'Timestamp when the file was ingested into bronze'
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# DBTITLE 1,Cell 7
# ─────────────────────────────────────────────────────────────────────────────
# LOAD THE CURRENT STATE OF THE CLASSIFICATION TABLE
# Read whatever has been classified so far into a pandas DataFrame.
# This is used later (in cell 14) to skip files that are already processed,
# making the pipeline idempotent (safe to re-run without duplicating rows).
# ─────────────────────────────────────────────────────────────────────────────
rawfiles_to_header = spark.read.format("delta").table("file_classification_from2016to2019")
rawfiles_to_header = rawfiles_to_header.toPandas()
print(f"Already classified: {len(rawfiles_to_header)} files")

# COMMAND ----------

# DBTITLE 1,Cell 10
# ─────────────────────────────────────────────────────────────────────────────
# KNOWN HEADER PATTERNS (the "dictionary" of valid file headers)
#
# Each entry maps a header group name → list of column names found in the
# first row of the file. This is how we identify what type of data a file
# contains, because different data exports have different column structures.
#
# Naming convention:
#   • header_one through header_seven: original patterns identified by Sebastian (2016-2017)
#   • header_08 and above: new patterns added by Wendy for unclassified files
#   • header_16: placeholder for files with no recognisable header (empty list)
#   • header_17+: patterns discovered during from2016to2019 classification
#
# FORMAT NOTE:
#   • Entries that are a list of multiple strings mean the file uses COMMAS as delimiter:
#       e.g. ['col_a', 'col_b', 'col_c'] → file line is: col_a,col_b,col_c
#   • Entries that are a list with ONE long semicolon-separated string mean SEMICOLONS:
#       e.g. ['col_a;col_b;col_c'] → file line is: col_a;col_b;col_c
#
# HOW TO ADD A NEW HEADER:
#   1. Run the classification pipeline (cells 13-15)
#   2. If "needs_review" files appear, the output prints the new column list
#   3. Copy that column list here as a new entry (e.g. 'header_20': [...])
#   4. Re-run from cell 9 onwards to reclassify
# ─────────────────────────────────────────────────────────────────────────────

unique_header_dict = {'header_one': 
                            ['Fecha de Liquidación', 'Fecha de Uso', 'Day Group Type', 'Hora Pico S/N', 'Fase',
                            'Emisor', 'Operador', 'Línea', 'Estación', 'Acceso de Estación', 'Dispositivo', 'Tipo de Tarjeta', 'Nombre de Perfil', 'Número de Tarjeta', 'Tipo de Tarifa', 'Saldo Previo a Transacción', 'Valor', 'Saldo Después de Transacción'],
                    'header_two': 
                          ['Fecha de Clearing;Fecha de Transaccion;Hora Pico SN;Fase;Emisor;Operador;Linea;Ruta;Parada;Tipo Vehiculo;ID Vehiculo;Dispositivo;Tipo Tarjeta;Nombre de Perfil;Numero Tarjeta;Tipo de Tarifa;Saldo Previo a Transaccion;Valor;Saldo Despues de Transaccion',],
                    'header_three': 
                          ['Fecha de Liquidación', 'Fecha de Uso', 'Day Group Type', 'Hora Pico S/N', 'Fase', 'Emisor', 'Operador', 'Línea', 'Ruta', 'Parada', 'Tipo de Vehículo', 'ID de Vehículo', 'Dispositivo', 'Tipo de Tarjeta', 'Nombre de Perfil', 'Número de Tarjeta', 'Tipo de Tarifa', 'Saldo Previo a Transacción', 'Valor', 'Saldo Después de Transacción'],
                    'header_four': 
                          ['Fecha de Clearing;Fecha de Transaccion;Day Group Type;Hora Pico SN;Fase;Emisor;Operador;Linea;Estacion;Acceso de Estación;Dispositivo;Tipo de Tarjeta;Nombre de Perfil;Numero de Tarjeta;Tipo de Tarifa;Saldo Previo a Transaccion;Valor;Saldo Despues de Transaccion',],
                    'header_five': 
                          ['Fecha de Clearing;Fecha de Transaccion;Hora Pico SN;Fase;Emisor;Operador;Linea;Ruta;Parada;Tipo Vehiculo;ID Vehiculo;Dispositivo;Tipo Tarjeta;Nombre de Perfil;Numero Tarjeta;Tipo de Tarifa;Saldo Previo a Transaccion;Valor;Saldo Despues de Transaccion;Ruta_Modificada;Linea_Modificada;Cenefa;Parada_Modificada',],
                    'header_six':
                           ['Fecha de Clearing', 'Fecha de Transaccion', 'DAY_GROUP_CD', 'Hora Pico SN', 'Fase', 'Emisor', 'Operador', 'Linea', 'Ruta', 'Parada', 'Tipo Vehiculo', 'ID Vehiculo', 'Dispositivo', 'Tipo Tarjeta', 'Nombre de Perfil', 'Numero Tarjeta', 'Tipo de Tarifa', 'Saldo Previo a Transaccion', 'Valor', 'Saldo Despues de Transaccion'],
                    'header_seven': 
                          ['Fecha de Clearing', 'Fecha de Transaccion', 'DAY_GROUP_CD', 'Hora Pico SN', 'Emisor', 'Operador', 'Linea', 'Estacion', 'Acceso de Estación', 'Dispositivo', 'Tipo de Tarjeta', 'Nombre de Perfil', 'Numero de Tarjeta', 'Tipo de Tarifa', 'Saldo Previo a Transaccion', 'Valor', 'Saldo Despues de Transaccion'],
                    'header_08': 
                        ['Unnamed: 0', 'Dispositivo', 'Emisor', 'Estacion_Parada', 'Fase', 'Fecha_Clearing', 'Fecha_Transaccion',  'Hora_Pico_SN', 'ID_Vehiculo', 'Linea', 'Nombre_Perfil', 'Numero_Tarjeta', 'Operador', 'Ruta', 'Saldo_Despues_Transaccion', 'Saldo_Previo_a_Transaccion', 'Sistema', 'Tipo_Tarifa', 'Tipo_Tarjeta', 'Tipo_Vehiculo', 'Valor'],
                    'header_09':
                         ['Dispositivo', 'Emisor', 'Estacion_Parada', 'Fase', 'Fecha_Clearing', 'Fecha_Transaccion', 'Hora_Pico_SN', 'ID_Vehiculo', 'Linea', 'Nombre_Perfil', 'Numero_Tarjeta', 'Operador', 'Ruta', 'Saldo_Despues_Transaccion', 'Saldo_Previo_a_Transaccion', 'Sistema', 'Tipo_Tarifa', 'Tipo_Tarjeta', 'Tipo_Vehiculo', 'Valor'],
                    'header_10': 
                        ['Dispositivo', 'Emisor', 'Estacion_Parada', 'Fase', 'Fecha_Clearing', 'Fecha_Transaccion', 'Hora_Pico_SN', 'ID_Vehiculo', 'Linea', 'Nombre_Perfil', 'Numero_Tarjeta', 'Operador', 'Ruta', 'Saldo_Despues_Transaccion', 'Saldo_Previo_a_Transaccion', 'Sistema', 'Tipo_Tarifa', 'Tipo_Tarjeta', 'Tipo_Vehiculo', 'Valor', 'archivo'],
                    'header_11': 
                        ['Acceso_Estacion', 'Day_Group_Type', 'Dispositivo', 'Emisor', 'Estacion_Parada', 'Fase', 'Fecha_Clearing', 'Fecha_Transaccion', 'Hora_Pico_SN', 'ID_Vehiculo', 'Linea', 'Nombre_Perfil', 'Numero_Tarjeta', 'Operador', 'Ruta', 'Saldo_Despues_Transaccion', 'Saldo_Previo_a_Transaccion', 'Sistema', 'Tipo_Tarifa', 'Tipo_Tarjeta', 'Tipo_Vehiculo', 'Valor', 'archivo'],
                    'header_12': 
                        ['Acceso_Estacion', 'Day_Group_Type', 'Dispositivo', 'Emisor', 'Estacion_Parada', 'Fase', 'Fecha_Clearing', 'Fecha_Transaccion', 'Hora_Pico_SN', 'ID_Vehiculo', 'Linea', 'Nombre_Perfil', 'Numero_Tarjeta', 'Operador', 'Ruta', 'Saldo_Despues_Transaccion', 'Saldo_Previo_a_Transaccion', 'Sistema', 'Tipo_Tarifa', 'Tipo_Tarjeta', 'Tipo_Vehiculo', 'Valor'],
                    'header_13': 
                        ['Unnamed: 0', 'Acceso_Estacion', 'Day_Group_Type', 'Dispositivo', 'Emisor', 'Estacion_Parada', 'Fase', 'Fecha_Clearing', 'Fecha_Transaccion', 'Hora_Pico_SN', 'ID_Vehiculo', 'Linea', 'Nombre_Perfil', 'Numero_Tarjeta', 'Operador', 'Ruta', 'Saldo_Despues_Transaccion', 'Saldo_Previo_a_Transaccion', 'Sistema', 'Tipo_Tarifa', 'Tipo_Tarjeta', 'Tipo_Vehiculo', 'Valor'],
                   'header_14':  
                       ['Unnamed: 0', 'Acceso_Estacion', 'Day_Group_Type', 'Dispositivo', 'Emisor', 'Estacion_Parada', 'Fase', 'Fecha_Clearing', 'Fecha_Transaccion', 'Hora_Pico_SN', 'Linea', 'Nombre_Perfil', 'Numero_Tarjeta', 'Operador', 'Saldo_Despues_Transaccion', 'Saldo_Previo_a_Transaccion', 'Tipo_Tarifa', 'Tipo_Tarjeta', 'Valor', 'ID_Vehiculo', 'Ruta', 'Tipo_Vehiculo', 'Sistema'],
                    'header_15': 
                        ['Unnamed: 0', 'Dispositivo', 'Emisor', 'Estacion_Parada', 'Fase', 'Fecha_Clearing', 'Fecha_Transaccion', 'Hora_Pico_SN', 'ID_Vehiculo', 'Linea', 'Nombre_Perfil', 'Numero_Tarjeta', 'Operador', 'Ruta', 'Saldo_Despues_Transaccion', 'Saldo_Previo_a_Transaccion', 'Tipo_Tarjeta', 'Tipo_Vehiculo', 'Valor', 'Sistema'],
                    'header_16': [],
                    'header_17':
                        ['fechaclearing', 'fechatransaccion', 'daygrouptype', 'fase', 'emisor', 'operador', 'linea', 'estacion', 'accesoestacion', 'dispositivo', 'tipotarjeta', 'nombreperfil', 'nrotarjeta', 'saldoprevioatransaccion', 'valor', 'saldodespuesdetransaccion'],
                    'header_18':
                        ['Fecha de Clearing', 'Fecha de Transaccion', 'Hora Pico SN', 'Emisor', 'Operador', 'Linea', 'Estacion', 'Acceso de Estación', 'Dispositivo', 'Tipo de Tarjeta', 'Nombre de Perfil', 'Numero de Tarjeta', 'Tipo de Tarifa', 'Saldo Previo a Transaccion', 'Valor', 'Saldo Despues de Transaccion'],
                    'header_19':
                        ['', 'Dispositivo', 'Emisor', 'Estacion_Parada', 'Fase', 'Fecha_Clearing', 'Fecha_Transaccion', 'Hora_Pico_SN', 'ID_Vehiculo', 'Linea', 'Nombre_Perfil', 'Numero_Tarjeta', 'Operador', 'Ruta', 'Saldo_Despues_Transaccion', 'Saldo_Previo_a_Transaccion', 'Sistema', 'Tipo_Tarjeta', 'Tipo_Vehiculo', 'Valor'],
}

broken_files = [""]  # not a real zip file



# COMMAND ----------

# ─────────────────────────────────────────────────────────────────────────────
# DEBUGGING HELPER (optional — leave commented out during normal runs)
#
# If the classification marks a file as 'broken' or 'needs_review' and you
# want to manually inspect its raw content, paste the file path below and
# uncomment these lines. It will print all lines so you can see what's inside.
# ─────────────────────────────────────────────────────────────────────────────

# file_test_is_broken = ''  # paste the full path of the file here

# with open(file_test_is_broken, "r") as text_file:
#     unknown = text_file.readlines()
# unknown

# COMMAND ----------

# DBTITLE 1,Explore raw data directory
# ─────────────────────────────────────────────────────────────────────────────
# EXPLORE THE RAW DATA DIRECTORY
# Before classifying, let's see what's in the folder:
#   • How many subfolders are there (each usually contains one type of file)
#   • How many "loose" files sit directly in the main folder
#   • What file extensions are present (csv, txt, zip, gz, xls, xlsx)
#
# This is an exploratory/informational cell — it doesn't modify anything.
# Delete it after the notebook is complete
# ─────────────────────────────────────────────────────────────────────────────

entry_path = os.path.join(V_DIR, "Workspace/Raw/from2016to2019")

# List everything at the top level of the directory
top_level = os.listdir(entry_path)

# Separate into subfolders vs individual files
subfolders = [item for item in top_level if os.path.isdir(os.path.join(entry_path, item))]
loose_files = [item for item in top_level if os.path.isfile(os.path.join(entry_path, item))]

# Collect the unique file extensions among loose files
loose_file_types = sorted(set(
    os.path.splitext(f)[1].lower().strip('.')
    for f in loose_files
    if os.path.splitext(f)[1]
))

# For each subfolder, collect the file extensions inside it
# (e.g. VALTRONCAL_01-06-2018 contains only .gz files)
subfolder_file_types = defaultdict(set)
for subfolder in subfolders:
    subfolder_path = os.path.join(entry_path, subfolder)
    for file in os.listdir(subfolder_path):
        n_files = len(os.listdir(subfolder_path))
        if os.path.isfile(os.path.join(subfolder_path, file)):
            ext = os.path.splitext(file)[1].lower().strip('.')
            if ext:
                subfolder_file_types[subfolder].add(ext)

print(f"Subfolders: {len(subfolders)}")
print(f"Loose files: {len(loose_files)}")
print(f"Loose file types: {loose_file_types}")
print(f"\nFile types per subfolder:")
for subfolder, types in subfolder_file_types.items():
    print(f"  {subfolder}: {sorted(types)}, {n_files} files")

tot_files = len(loose_files) + sum(len(os.listdir(os.path.join(entry_path, subfolder))) for subfolder in subfolders)
print(f"\nTotal files: {tot_files}")



# COMMAND ----------

# DBTITLE 1,Classification helpers
# ─────────────────────────────────────────────────────────────────────────────
# CLASSIFICATION HELPERS
# These functions read the first row (header) of each raw file and try to match
# it against the known header patterns defined in unique_header_dict (cell 10).
# ─────────────────────────────────────────────────────────────────────────────

import os, gzip, zipfile, json, chardet, csv
from io import StringIO, BytesIO
from collections import defaultdict


# ── Step 1: Prepare the lookup table from unique_header_dict ────────────────
# We need to convert each entry in unique_header_dict into a standardised
# format (a tuple of column names) so we can compare them against file headers.

def _normalise_header_entry(cols):
    """
    Convert a unique_header_dict value into a comparable tuple of column names.
    
    Why this is needed:
    - Some dict entries store headers as a Python list of column names:
        e.g. ['Fecha de Clearing', 'Fecha de Transaccion', ...]
    - Others store them as a SINGLE string with semicolons inside:
        e.g. ['Fecha de Clearing;Fecha de Transaccion;...']
      (this happens when the original file uses ; as delimiter)
    
    This function normalises both formats into the same shape:
        ('Fecha de Clearing', 'Fecha de Transaccion', ...)
    """
    if not cols:
        return ()  # empty entry (like header_16) → empty tuple
    # Case: single string with semicolons → split it into individual columns
    if len(cols) == 1 and ';' in cols[0]:
        return tuple(c.strip() for c in cols[0].split(';'))
    # Case: already a list of column names → just strip whitespace
    return tuple(c.strip() for c in cols)


def _normalize_header_values(values):
    """
    Normalize parsed column names:
      - Strip BOM characters (\ufeff) that some Windows files prepend
      - Rename empty-string columns to 'Unnamed: 0', 'Unnamed: 1', etc.
        (this matches how pandas names unnamed columns, and keeps
        consistency with header_08, header_13, header_14, header_15)
    """
    return [
        f'Unnamed: {i}' if str(col).strip('\ufeff') == '' else str(col).strip('\ufeff')
        for i, col in enumerate(values)
    ]

# Build a reverse lookup dictionary:
#   key   = normalised tuple of column names
#   value = header group name (e.g. 'header_one', 'header_17')
# This lets us quickly check: "does this file's header match any known pattern?"
norm_header_lookup = {
    _normalise_header_entry(v): k
    for k, v in unique_header_dict.items()
    if v  # skip entries with empty lists (like header_16)
}

# ── Step 2: Read the first line (header row) of any file ────────────────────
# Files come in different formats: plain .csv/.txt, compressed .gz, or .zip.
# This function handles all three and returns the header line as a string,
# along with metadata about encoding and archive format.

def _prefer_utf8(raw_bytes, chardet_encoding):
    """
    When chardet reports ISO-8859-1 / latin-1, try UTF-8 first.
    Rationale: latin-1 can decode ANY byte sequence without errors, so chardet
    often picks it even when the file is valid UTF-8. If UTF-8 decodes cleanly,
    it's almost certainly the correct encoding.
    """
    enc = (chardet_encoding or 'latin-1').lower().replace('-', '').replace('_', '')
    if enc in ('iso88591', 'latin1'):
        try:
            raw_bytes.decode('utf-8')
            return 'utf-8'
        except (UnicodeDecodeError, ValueError):
            pass
    return chardet_encoding or 'latin-1'


def _read_first_line(filepath):
    """
    Open a file (plain, gzipped, or zipped), read its first line, and detect
    its text encoding.

    Returns a tuple of 6 values:
      - first_line:       the header row as a decoded string (or None if unreadable)
      - encoding:         the character encoding detected (e.g. 'utf-8', 'latin-1')
      - encoding_source:  how we determined the encoding ('chardet_file', 'chardet_zip_member')
      - zipped:           1 if the file is a zip archive, 0 otherwise
      - archive_format:   'zip', 'gz', or 'none'
      - inner_file_name:  for zip files, the name of the file inside the archive
    """
    ext = os.path.splitext(filepath)[1].lower()

    # --- Handle .zip files ---
    # A zip archive can contain multiple files; we take the first one.
    if ext == '.zip':
        try:
            with zipfile.ZipFile(filepath, 'r') as zf:
                inner = zf.namelist()[0]           # name of the first file inside
                with zf.open(inner) as f:
                    raw = f.read(8192)             # read first 8KB (enough for header)
                det = chardet.detect(raw)          # guess the text encoding
                enc = _prefer_utf8(raw, det.get('encoding'))
                line = raw.decode(enc, errors='replace').split('\n')[0]
                return line.strip(), enc, 'chardet_zip_member', 1, 'zip', inner
        except (zipfile.BadZipFile, IndexError):
            # File is corrupt or empty zip
            return None, None, None, 1, 'zip', None

    # --- Handle .gz (gzip) files ---
    elif ext == '.gz':
        try:
            with gzip.open(filepath, 'rb') as f:
                raw = f.read(8192)
            det = chardet.detect(raw)
            enc = _prefer_utf8(raw, det.get('encoding'))
            line = raw.decode(enc, errors='replace').split('\n')[0]
            return line.strip(), enc, 'chardet_file', 0, 'gz', None
        except Exception:
            return None, None, None, 0, 'gz', None

    # --- Handle plain text files (.csv, .txt, etc.) ---
    else:
        with open(filepath, 'rb') as f:
            raw = f.read(8192)
        det = chardet.detect(raw)
        enc = _prefer_utf8(raw, det.get('encoding'))
        line = raw.decode(enc, errors='replace').split('\n')[0]
        # Remove BOM (Byte Order Mark) if present — some Windows files start with it
        if line.startswith('\ufeff'):
            line = line[1:]
        return line.strip(), enc, 'chardet_file', 0, 'none', None


# ── Step 3: Match a header line against the known header patterns ───────────
# Raw files in this dataset use either semicolons (;) or commas (,) as their
# column delimiter. We don't know which one a file uses until we try both.

def _match_header(raw_line):
    """
    Try to split the raw header line using both ; and , as delimiters,
    and check if the resulting column list matches any known header pattern.

    Returns a tuple of 3 values:
      - header_name:   the matched header group (e.g. 'header_one') or None
      - delimiter:     the delimiter that worked (';' or ',')
      - fields_tuple:  the parsed column names as a tuple
    """
    # First pass: try each delimiter and look for an exact match
    for delim in [';', ',']:
        # csv.reader handles quoted fields correctly
        # e.g. '"Fecha de Liquidación","Fecha de Uso"' → ['Fecha de Liquidación', 'Fecha de Uso']
        reader = csv.reader(StringIO(raw_line), delimiter=delim)
        fields = next(reader, [])
        # Normalize: strip whitespace + rename empty cols to 'Unnamed: N'
        fields_normalized = tuple(_normalize_header_values([f.strip() for f in fields]))
        if len(fields_normalized) <= 1:
            continue  # this delimiter didn't split anything → wrong delimiter
        # Check if this matches any known header
        if fields_normalized in norm_header_lookup:
            return norm_header_lookup[fields_normalized], delim, fields_normalized

    # Second pass: no match found, but still parse the line for reporting
    # (we'll flag these files as 'needs_review' so the user can inspect them)
    for delim in [';', ',']:
        reader = csv.reader(StringIO(raw_line), delimiter=delim)
        fields = next(reader, [])
        if len(fields) > 1:
            return None, delim, tuple(_normalize_header_values([f.strip() for f in fields]))

    # Fallback: couldn't parse at all
    return None, ',', (raw_line,)


# ── Step 4: Main classification function ────────────────────────────────────
# This is the function that brings everything together: for a given file path,
# it reads the header, matches it, and returns a row ready to insert into
# the Delta table.

def classify_file(filepath, already_classified_set):
    """
    Classify a single file by reading its header and matching against known patterns.

    Parameters:
      - filepath:               full path to the file to classify
      - already_classified_set: set of file paths that are already in the Delta table
                                (so we don't process them again)

    Returns:
      - A dictionary with all columns for the Delta table, OR
      - None if the file should be skipped (already classified or unsupported format)
    """
    # Skip files that are already in the classification table
    if filepath in already_classified_set:
        return None

    ext = os.path.splitext(filepath)[1].lower().lstrip('.')

    # Extensionless files are treated as CSV (common in this dataset:
    # e.g. "valtroncal_01nov2017_ZACARACAS" is a plain CSV without .csv extension)
    if ext == '':
        ext = 'csv'

    # ── Excel files (.xls, .xlsx): use pandas to read the header ──
    # Excel is a binary format — we can't read it as raw text like CSV.
    # Instead, we use pd.read_excel(nrows=0) to get just the column names.
    if ext in ('xls', 'xlsx'):
        try:
            excel_df = pd.read_excel(filepath, nrows=0)
            excel_header = _normalize_header_values(list(excel_df.columns))
            header_name_xl = norm_header_lookup.get(tuple(excel_header))
            return {
                'raw_filepath': filepath,
                'header': header_name_xl,
                'source_period': 'from2016to2019',
                'zipped': 0,
                'file_format': ext,
                'archive_format': 'none',
                'delimiter': None,  # not applicable for Excel
                'encoding': None,   # not applicable for Excel
                'encoding_source': 'excel_native',
                'transform_format': header_name_xl,
                'inner_file_name': None,
                'header_row': 1,
                'normalized_header': json.dumps(excel_header, ensure_ascii=False),
                'classification_status': 'ready' if header_name_xl else 'needs_review',
                'detection_notes': None if header_name_xl else 'No matching header in unique_header_dict',
                'detected_at': pd.Timestamp.utcnow(),
                'ingested_at': None,
            }
        except Exception as e:
            return {
                'raw_filepath': filepath,
                'header': None,
                'source_period': 'from2016to2019',
                'zipped': 0,
                'file_format': ext,
                'archive_format': 'none',
                'delimiter': None,
                'encoding': None,
                'encoding_source': None,
                'transform_format': None,
                'inner_file_name': None,
                'header_row': 1,
                'normalized_header': None,
                'classification_status': 'broken',
                'detection_notes': f'Excel read failed: {type(e).__name__}: {e}',
                'detected_at': pd.Timestamp.utcnow(),
                'ingested_at': None,
            }

    # ── Guard: detect 0-byte (empty) files ──
    # These are broken/corrupt files with no content at all.
    # We mark them as 'broken' immediately instead of trying to read them.
    file_size = os.path.getsize(filepath)
    if file_size == 0:
        return {
            'raw_filepath': filepath,
            'header': None,
            'source_period': 'from2016to2019',
            'zipped': 0,
            'file_format': ext,
            'archive_format': 'none',
            'delimiter': None,
            'encoding': None,
            'encoding_source': None,
            'transform_format': None,
            'inner_file_name': None,
            'header_row': 1,
            'normalized_header': None,
            'classification_status': 'broken',
            'detection_notes': '0-byte empty file',
            'detected_at': pd.Timestamp.utcnow(),
            'ingested_at': None,
        }

    # ── Read the header row from the file ──
    result = _read_first_line(filepath)
    raw_line, enc, enc_source, zipped, archive_fmt, inner_name = result

    # If we couldn't read the first line (corrupt archive or empty content),
    # try pandas as a fallback before giving up
    if raw_line is None or raw_line == '':
        # Pandas fallback: sometimes pd.read_csv can handle files that manual
        # reading cannot (e.g. unusual compression, BOM issues)
        try:
            fallback_df = pd.read_csv(filepath, nrows=0)
            fallback_header = _normalize_header_values(list(fallback_df.columns))
            fallback_delim = ';' if len(fallback_header) == 1 and ';' in fallback_header[0] else ','
            header_name_fb = norm_header_lookup.get(tuple(fallback_header))
            return {
                'raw_filepath': filepath,
                'header': header_name_fb,
                'source_period': 'from2016to2019',
                'zipped': zipped or 0,
                'file_format': 'csv',
                'archive_format': archive_fmt or 'none',
                'delimiter': fallback_delim,
                'encoding': enc,
                'encoding_source': 'pandas_fallback',
                'transform_format': header_name_fb,
                'inner_file_name': inner_name,
                'header_row': 1,
                'normalized_header': json.dumps(fallback_header, ensure_ascii=False),
                'classification_status': 'ready' if header_name_fb else 'needs_review',
                'detection_notes': 'pandas_fallback_after_empty_first_line',
                'detected_at': pd.Timestamp.utcnow(),
                'ingested_at': None,
            }
        except Exception:
            pass  # pandas also failed → truly broken

        return {
            'raw_filepath': filepath,
            'header': None,
            'source_period': 'from2016to2019',
            'zipped': zipped or 0,
            'file_format': ext,
            'archive_format': archive_fmt or 'none',
            'delimiter': None,
            'encoding': enc,
            'encoding_source': enc_source,
            'transform_format': None,
            'inner_file_name': inner_name,
            'header_row': 1,
            'normalized_header': None,
            'classification_status': 'broken',
            'detection_notes': 'Could not read first line' if raw_line is None else 'Empty first line (no header)',
            'detected_at': pd.Timestamp.utcnow(),
            'ingested_at': None,
        }

    # ── Try to match the header against known patterns ──
    header_name, delimiter, norm_tuple = _match_header(raw_line)

    # If matched → 'ready' (file can be ingested)
    # If not matched → 'needs_review' (user should inspect and possibly add to dict)
    status = 'ready' if header_name else 'needs_review'
    notes = None if header_name else 'No matching header in unique_header_dict'

    return {
        'raw_filepath': filepath,
        'header': header_name,
        'source_period': 'from2016to2019',
        'zipped': zipped,
        'file_format': ext if archive_fmt == 'none' else 'csv',
        'archive_format': archive_fmt,
        'delimiter': delimiter,
        'encoding': enc,
        'encoding_source': enc_source,
        'transform_format': header_name,
        'inner_file_name': inner_name,
        'header_row': 1,
        'normalized_header': json.dumps(list(norm_tuple), ensure_ascii=False),
        'classification_status': status,
        'detection_notes': notes,
        'detected_at': pd.Timestamp.utcnow(),
        'ingested_at': None,
    }

print("Classification helpers ready.")

# COMMAND ----------

# DBTITLE 1,Classify all files and write to Delta
# ─────────────────────────────────────────────────────────────────────────────
# CLASSIFY ALL FILES
# This cell does 3 things:
#   1. Collects all file paths we want to classify (csv, txt, zip, gz)
#   2. Skips files that are already in the Delta table (idempotent — safe to re-run)
#   3. Classifies new files and shows a summary
# ─────────────────────────────────────────────────────────────────────────────

entry_path = os.path.join(V_DIR, "Workspace/Raw/from2016to2019")

# ── Part A: Gather all file paths that need classification ──────────────────
all_files = []

# A.1 — Loose files sitting directly in the entry_path folder
# Includes ALL file types: .csv, .txt, .zip, .gz, .xls, .xlsx, and extensionless
# files (which are often CSVs without the .csv extension,
# e.g. "valtroncal_01nov2017_ZACARACAS" — see explore-catalog notebook for details)
for fname in os.listdir(entry_path):
    fpath = os.path.join(entry_path, fname)
    if os.path.isfile(fpath):
        all_files.append(fpath)

# A.2 — Files inside subfolders (e.g. VALTRONCAL_01-06-2018/*.gz, 2019ER10074/*.zip)
for subfolder in os.listdir(entry_path):
    subfolder_path = os.path.join(entry_path, subfolder)
    if os.path.isdir(subfolder_path):
        for fname in os.listdir(subfolder_path):
            fpath = os.path.join(subfolder_path, fname)
            if os.path.isfile(fpath):
                all_files.append(fpath)

print(f"Total target files found: {len(all_files)}")

# ── Part B: Build a set of paths already in the Delta table ─────────────────
# This makes the cell idempotent: if you run it again, it won't re-classify
# files that were already processed in a previous run.
already_classified = set(rawfiles_to_header['raw_filepath'].tolist()) if len(rawfiles_to_header) > 0 else set()
print(f"Already classified (will skip): {len(already_classified)}")

# ── Part C: Loop over all files and classify each one ──────────────────────
results = []   # will hold the new classification rows (dicts)
skipped = 0    # counter for files we skipped

for fpath in tqdm(all_files, desc="Classifying files"):
    row = classify_file(fpath, already_classified)
    if row is None:
        # classify_file returns None when the file is already classified
        # or has an unsupported extension
        skipped += 1
    else:
        results.append(row)

print(f"\nNewly classified: {len(results)}")
print(f"Skipped (already in table or other problem): {skipped}")

# ── Part D: Show summary of what happened ─────────────────────────────────
if results:
    df_new = pd.DataFrame(results)
    status_counts = df_new['classification_status'].value_counts()
    header_counts = df_new['header'].value_counts(dropna=False)
    print(f"\n── Status breakdown ──")
    print(status_counts.to_string())
    # 'ready' = matched a known header → good to ingest
    # 'needs_review' = header not recognised → inspect and add to unique_header_dict
    # 'broken' = file is empty or unreadable
    print(f"\n── Header group breakdown ──")
    print(header_counts.to_string())

# COMMAND ----------

# DBTITLE 1,Review needs_review files
# Show details for files that didn't match any known header pattern
needs_review = df_new[df_new['classification_status'] == 'needs_review']
for _, row in needs_review.iterrows():
    print(f"File: {row['raw_filepath']}")
    print(f"Format: {row['file_format']} | Delimiter: {row['delimiter']} | Encoding: {row['encoding']}")
    print(f"Header detected:")
    print(f"  {row['normalized_header']}")
    print()

# COMMAND ----------

# DBTITLE 1,Write new classifications to Delta and show unmatched headers
# ─────────────────────────────────────────────────────────────────────────────
# WRITE RESULTS TO DELTA TABLE & REPORT UNMATCHED HEADERS
# This cell:
#   1. Appends newly classified rows to the Delta table
#   2. Reports any files whose headers didn't match any known pattern,
#      printing the exact column list so you can add them to unique_header_dict
# ─────────────────────────────────────────────────────────────────────────────

# ── Part A: Validate and save to Delta ───────────────────────────────────────
if results:
    sdf = spark.createDataFrame(df_new)

    # SAFETY CHECK: assert that the DataFrame columns match the Delta table schema
    # exactly (order matters for insertInto). This prevents silent data corruption
    # if columns are reordered or renamed in the future.
    table_cols = [f.name for f in spark.table("prd_mega.scolom15.file_classification_from2016to2019").schema.fields]
    df_cols = sdf.columns
    assert list(df_cols) == table_cols, (
        f"Column order mismatch — fix df_new columns to match the table schema.\n"
        f"  Table:     {table_cols}\n"
        f"  DataFrame: {list(df_cols)}"
    )

    # SAFETY CHECK: no duplicate file paths in the batch we're about to write
    assert df_new['raw_filepath'].is_unique, (
        f"Duplicate raw_filepath values detected in new results! "
        f"({len(df_new) - df_new['raw_filepath'].nunique()} duplicates)"
    )

    # Append new rows (mode="append" adds without touching existing ones)
    sdf.write.format("delta").mode("append").saveAsTable("file_classification_from2016to2019")
    print(f"✓ Appended {len(results)} rows to prd_mega.scolom15.file_classification_from2016to2019")

    # ── Validate zip files for ingestion compatibility ──
    # Each zip must contain exactly one file — multi-file zips would cause
    # issues during downstream ingestion (we only extract the first member).
    zip_rows = df_new[df_new['archive_format'] == 'zip']
    if len(zip_rows) > 0:
        bad_zips = []
        for zip_path in zip_rows['raw_filepath']:
            try:
                with zipfile.ZipFile(zip_path, 'r') as zf:
                    n = len(zf.namelist())
                    if n != 1:
                        bad_zips.append((zip_path, n))
            except zipfile.BadZipFile:
                bad_zips.append((zip_path, -1))
        if bad_zips:
            print(f"\n⚠️  {len(bad_zips)} problematic zip file(s) found:")
            for path, n in bad_zips:
                issue = f"{n} members" if n > 0 else "corrupt zip"
                print(f"    {os.path.basename(path)}: {issue}")
            print("  Consider adding these to broken_files if they can't be ingested.")
        else:
            print(f"✓ All {len(zip_rows)} zip files contain exactly 1 member.")
else:
    print("Nothing new to write — all files already classified.")

# ── Part B: Report unmatched headers ────────────────────────────────────────
# If some files have 'needs_review' status, it means their header row didn't
# match ANY entry in unique_header_dict. This section prints the new patterns
# so you can inspect them and decide whether to add them to the dictionary.
#
# ACTION REQUIRED when you see "needs_review" files:
#   1. Look at the printed column lists below
#   2. Add matching entries to unique_header_dict in cell 10 (e.g. 'header_20': [...])
#   3. Delete the needs_review rows from the Delta table:
#        DELETE FROM file_classification_from2016to2019 WHERE classification_status = 'needs_review'
#   4. Re-run cells 9 → 10 → 13 → 14 → 15 to reclassify those files
if results:
    unmatched = df_new[df_new['classification_status'] == 'needs_review']
    if len(unmatched) > 0:
        # Auto-compute the next available header key number
        # e.g. if the highest existing key is 'header_19', suggest 'header_20'
        num_keys = [int(k[7:]) for k in unique_header_dict
                    if k.startswith('header_') and k[7:].replace('_', '').isdigit()]
        next_num = max(num_keys, default=0) + 1

        # De-duplicate: show each distinct header pattern only once
        unique_unmatched = unmatched.drop_duplicates(subset='normalized_header')[['normalized_header', 'raw_filepath', 'delimiter']]
        n_files = len(unmatched)
        n_patterns = len(unique_unmatched)

        sep = '=' * 64
        print(f"\n{sep}")
        print(f"  ⚠️  {n_files} file(s) have {n_patterns} NEW HEADER PATTERN(S) — ACTION REQUIRED")
        print(sep)
        print(f"\n── New header patterns to add to unique_header_dict (cell 10) ──")
        for idx, (i, row) in enumerate(unique_unmatched.iterrows()):
            cols = json.loads(row['normalized_header'])
            key = f"header_{next_num + idx}"
            n_with_pattern = len(unmatched[unmatched['normalized_header'] == row['normalized_header']])
            print(f"\n  Suggested key: '{key}'")
            print(f"    Columns ({len(cols)}, delim='{row['delimiter']}'):")
            print(f"      {cols}")
            print(f"    Files with this pattern: {n_with_pattern}")
            print(f"    Example: {os.path.basename(row['raw_filepath'])}")
        print(f"\n{sep}")
        print(f"  Steps:")
        print(f"    1. Add the entry/entries above to unique_header_dict in cell 10")
        print(f"    2. DELETE FROM file_classification_from2016to2019 WHERE classification_status = 'needs_review'")
        print(f"    3. Re-run cells 9 → 10 → 13 → 14 → 15")
        print(sep)
    else:
        print("\n✓ All files matched an existing header in unique_header_dict.")
