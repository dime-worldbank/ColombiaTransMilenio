# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Cell 1
# MAGIC %md
# MAGIC # Classify files by header (since 2020)
# MAGIC Classifies raw CSV/zip files into header groups, detects per-file import parameters (encoding, delimiter, archive format), and records results in the `file_classification_since2020` Delta table.  
# MAGIC No physical file copying — files stay in their original location.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cell 2
# Pip install non-standard packages
%pip install tqdm chardet

import os
import csv
import zipfile
from tqdm import tqdm
import pandas as pd
from random import seed
import chardet

# COMMAND ----------

# MAGIC %run ./utils/handle_files

# COMMAND ----------

# DBTITLE 1,Cell 4
# Directories
S_DIR = '/Volumes/prd_csc_mega/sColom15/'
V_DIR = f'{S_DIR}vColom15/'
user = 'wbrau@worldbank.org'
git = f'/Workspace/Users/{user}/ColombiaTransMilenio'

# Important sub-directories for this notebook
raw_dir = V_DIR + '/Workspace/Raw/'


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC /* Set default catalog and schema */
# MAGIC
# MAGIC USE CATALOG prd_mega;
# MAGIC USE SCHEMA scolom15;
# MAGIC
# MAGIC SELECT
# MAGIC   current_catalog() as current_catalog,
# MAGIC   current_schema()  as current_schema;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cell 6
# MAGIC %sql
# MAGIC -- Create table to list filename, detected import metadata, and header if it does not exist
# MAGIC CREATE TABLE IF NOT EXISTS file_classification_since2020 (
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
# Read existing classification table
rawfiles_to_header = spark.read.format("delta").table("file_classification_since2020")
rawfiles_to_header = rawfiles_to_header.toPandas()
print(f"Already classified: {len(rawfiles_to_header)} files")

# COMMAND ----------

# DBTITLE 1,Cell 8
# Parameters
# Old list of headers by Sebastian for 2016-2017 data:  letters (one - seven)
# New list of headers by Wendy for data since 2020: numbers (since 8)

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
                    'header_16': []}

broken_files = ["validacionDual20230630.csv",
                "validacionTroncal20200725.csv",
                "validacionZonal20200601.csv",
                "validacionZonal20220628.csv",
                "validacionTroncal20260507.zip"]  # not a real zip file



# COMMAND ----------

# # Uncomment this to test for broken files

#file_test_is_broken = '' # complete path of the file goes here

#with open(file_test_is_broken, "r") as text_file:
#    unknown = text_file.readlines()
#unknown

# COMMAND ----------

# List all rawfiles
all_raw_filepaths = []

for v in ['ValidacionDual/', 'ValidacionTroncal/', 'ValidacionZonal/' ]:        ## Falta Cable!!
    file_dir       = f'/{raw_dir}/since2020/{v}/'
    filenames       = os.listdir(file_dir)
    raw_filepaths_v = [file_dir + filename for filename in filenames]

    all_raw_filepaths += raw_filepaths_v



# Keep the ones we should classify
not_classified = all_raw_filepaths # will reclassify
not_classified = list(set(all_raw_filepaths).difference(rawfiles_to_header.raw_filepath))
not_classified_not_broken = [f for f in not_classified if os.path.basename(f) not in broken_files]
n_to_classify = len(not_classified_not_broken)
print(f"{n_to_classify} files to classify")

# COMMAND ----------

# DBTITLE 1,Cell 11
if n_to_classify > 0:
    
    # Check no duplicates in original file paths
    old_and_new_raw_filepaths = list(rawfiles_to_header.raw_filepath) + not_classified_not_broken
    assert len(old_and_new_raw_filepaths) == len(set(old_and_new_raw_filepaths)), "Duplicate raw filepaths detected!"

    # Detect headers by reading first row of each file and persist the actual parsing path used
    import io
    import json
    from collections import Counter

    detected_as_zip = set()  # files confirmed zip-compressed via magic bytes
    headers = []
    header_scan_records = []
    skipped_files = []
    skipped_metadata = []
    candidate_delimiters = [',', ';']
    known_header_lookup = {tuple(v): k for k, v in unique_header_dict.items()}
    transform_format_lookup = {
        'header_08': 'format_6',
        'header_09': 'format_6',
        'header_10': 'format_6',
        'header_11': 'format_7',
        'header_12': 'format_7',
        'header_13': 'format_7',
        'header_14': 'format_7',
        'header_15': 'format_6'
    }

    def normalize_header_values(values):
        return [
            f'Unnamed: {i}' if str(col).strip('\ufeff') == '' else str(col).strip('\ufeff')
            for i, col in enumerate(values)
        ]

    def choose_header_candidate(header_line):
        parsed_candidates = {
            delimiter: normalize_header_values(next(csv.reader([header_line], delimiter=delimiter), []))
            for delimiter in candidate_delimiters
        }
        matched_candidates = {
            delimiter: values
            for delimiter, values in parsed_candidates.items()
            if tuple(values) in known_header_lookup
        }
        if matched_candidates:
            chosen_delimiter, chosen_header = max(
                matched_candidates.items(),
                key=lambda item: (len(item[1]), 1 if item[0] == ',' else 0)
            )
            matched_known_header = True
        else:
            chosen_delimiter, chosen_header = max(
                parsed_candidates.items(),
                key=lambda item: (len(item[1]), 1 if item[0] == ',' else 0)
            )
            matched_known_header = False
        return chosen_delimiter, chosen_header, parsed_candidates, matched_known_header

    for f in tqdm(not_classified_not_broken):
        archive_format = None
        inner_name = None
        enc = None
        encoding_source = None
        detection_notes = []
        detected_at = pd.Timestamp.utcnow()
        try:
            with open(f, 'rb') as fb:
                file_signature = fb.read(4)
            is_zip = file_signature[:2] == b'PK'

            if is_zip:
                archive_format = 'zip'
                detected_as_zip.add(f)
                with zipfile.ZipFile(f, 'r') as zf:
                    inner_names = zf.namelist()
                    if not inner_names:
                        raise ValueError('zip archive has no members')
                    inner_name = inner_names[0]
                    if len(inner_names) != 1:
                        detection_notes.append(f'zip_member_count={len(inner_names)}')

                    with zf.open(inner_name) as inner_file:
                        raw_sample = inner_file.read(4096)
                    enc = chardet.detect(raw_sample).get('encoding')
                    if enc is None or enc.lower() == 'unknown':
                        enc = 'latin-1'
                        encoding_source = 'fallback_latin-1'
                    else:
                        encoding_source = 'chardet_zip_member'

                    with zf.open(inner_name) as inner_file:
                        header_line = io.TextIOWrapper(inner_file, encoding=enc, errors='replace').readline().strip('\r\n')
            else:
                with open(f, 'rb') as fb:
                    raw_sample = fb.read(4096)
                enc = chardet.detect(raw_sample).get('encoding')
                if enc is None or enc.lower() == 'unknown':
                    enc = 'latin-1'
                    encoding_source = 'fallback_latin-1'
                else:
                    encoding_source = 'chardet_file'

                with open(f, encoding=enc, errors='replace') as fin:
                    header_line = fin.readline().strip('\r\n')

            delimiter, chosen_header, parsed_candidates, matched_known_header = choose_header_candidate(header_line)
            if matched_known_header:
                detection_notes.append(f'matched_known_header_with_delimiter={delimiter}')
            else:
                detection_notes.append('no_known_header_match_during_parse')
            if delimiter == ';' and len(parsed_candidates.get(',', [])) == 1 and len(chosen_header) > 1:
                detection_notes.append('semicolon_split_recovered_header_structure')

            headers.append(chosen_header)
            header_scan_records.append({
                'raw_filepath': f,
                'source_period': 'since2020',
                'zipped': int(is_zip),
                'file_format': 'csv',
                'archive_format': archive_format,
                'delimiter': delimiter,
                'encoding': enc,
                'encoding_source': encoding_source,
                'inner_file_name': inner_name,
                'header_row': 1,
                'normalized_header': json.dumps(chosen_header, ensure_ascii=False),
                'detection_notes': '; '.join(detection_notes) if detection_notes else None,
                'detected_at': detected_at,
                'ingested_at': pd.NaT
            })
        except Exception as read_error:
            try:
                csvin = pd.read_csv(f, nrows=0)  # handles zip files
                fallback_header = normalize_header_values(list(csvin.columns))
                fallback_delimiter = ';' if len(fallback_header) == 1 and ';' in fallback_header[0] else ','
                headers.append(fallback_header)
                header_scan_records.append({
                    'raw_filepath': f,
                    'source_period': 'since2020',
                    'zipped': int(archive_format == 'zip'),
                    'file_format': 'csv',
                    'archive_format': archive_format,
                    'delimiter': fallback_delimiter,
                    'encoding': enc,
                    'encoding_source': encoding_source or 'pandas_fallback',
                    'inner_file_name': inner_name,
                    'header_row': 1,
                    'normalized_header': json.dumps(fallback_header, ensure_ascii=False),
                    'detection_notes': f"pandas_fallback_after={type(read_error).__name__}: {read_error}",
                    'detected_at': detected_at,
                    'ingested_at': pd.NaT
                })
            except Exception as pandas_error:
                print(f"WARNING: skipping unreadable file: {os.path.basename(f)}")
                skipped_files.append(f)
                skipped_metadata.append({
                    'raw_filepath': f,
                    'header': 'broken',
                    'source_period': 'since2020',
                    'zipped': int(archive_format == 'zip' or f.endswith('.zip')),
                    'file_format': 'csv',
                    'archive_format': archive_format,
                    'delimiter': None,
                    'encoding': enc,
                    'encoding_source': encoding_source or 'header_detection_failed',
                    'transform_format': None,
                    'inner_file_name': inner_name,
                    'header_row': None,
                    'normalized_header': None,
                    'classification_status': 'broken',
                    'detection_notes': f"header_detection_failed={type(read_error).__name__}: {read_error}; pandas_fallback_failed={type(pandas_error).__name__}: {pandas_error}",
                    'detected_at': detected_at,
                    'ingested_at': pd.NaT
                })

    # Remove skipped files from classification list
    if skipped_files:
        not_classified_not_broken = [f for f in not_classified_not_broken if f not in skipped_files]
        print(f"Skipped {len(skipped_files)} unreadable files")

    # Headers are already normalized during parsing

    # See how many unique headers we found
    seed(510)
    unique_headers = list(set(tuple(x) for x in headers)) 
    print(f'Unique headers found: {len(unique_headers)}')
    for x in range(len(unique_headers)):
        head = unique_headers[x] 
        print(f'----------------')
        print(sum([h == list(head) for h in headers]), "files")
        print(head)

    # Check all detected headers are in our known dictionary
    unknown_headers = [val for val in unique_headers if list(val) not in list(unique_header_dict.values())]
    if unknown_headers:
        # Auto-compute next sequential key (e.g. header_17)
        num_keys = [int(k[7:]) for k in unique_header_dict if k.startswith('header_') and k[7:].isdigit()]
        next_num = max(num_keys, default=0) + 1

        sep = '=' * 64
        msg = f"\n{sep}\n  {len(unknown_headers)} NEW HEADER(S) DETECTED — ACTION REQUIRED\n{sep}\n"
        for i, h in enumerate(unknown_headers):
            key = f"header_{next_num + i:02d}"
            examples = [os.path.basename(f) for f, hdr in zip(not_classified_not_broken, headers)
                        if hdr == list(h)][:3]
            msg += f"\nStep 1a — add to unique_header_dict in Cell 8:\n"
            msg += f"  '{key}':\n      {list(h)},\n"
            msg += f"  ({len([f for f, hdr in zip(not_classified_not_broken, headers) if hdr == list(h)])} file(s), e.g. {examples[0]})\n"
        msg += f"\nStep 1b — add a transform_{next_num:02d}() method in utils/handle_files (loaded by Cell 3)\n"
        msg += f"Step 2  — re-run from Cell 8\n"
        msg += sep
        raise AssertionError(msg)

    assert len(not_classified_not_broken) == len(headers) == len(header_scan_records)

    # Map each file to its header group
    file_header_dict_inv = {}
    for file, header in zip(not_classified_not_broken, headers):
        for key, value in unique_header_dict.items():
            if header == value:  
                file_header_dict_inv[file] = key
                break  

    unmatched = [f for f in not_classified_not_broken if f not in file_header_dict_inv]
    assert not unmatched, f"{len(unmatched)} files had no header match:\n" + "\n".join(os.path.basename(f) for f in unmatched[:10])

    # Summary of files per header
    header_counts = Counter(file_header_dict_inv.values())
    for key, cnt in sorted(header_counts.items()):
        print(f"{key}: {cnt}")

    # Build classification dataframe
    file_to_header_df = pd.DataFrame(header_scan_records)
    file_to_header_df["header"] = file_to_header_df.raw_filepath.map(file_header_dict_inv)
    file_to_header_df["transform_format"] = file_to_header_df["header"].map(transform_format_lookup)
    file_to_header_df["classification_status"] = file_to_header_df["header"].apply(
        lambda x: 'unsupported_empty_header' if x == 'header_16' else ('ready' if pd.notna(transform_format_lookup.get(x)) else 'needs_review')
    )

    # Validate: every file got a header assigned
    assert file_to_header_df.header.isin(unique_header_dict.keys()).all(), "Some files have no header match!"
    assert file_to_header_df.header.notna().all(), "Null headers found — check file_header_dict_inv"

    # Validate: each zip file contains exactly one file (needed for ingestion)
    zip_files = file_to_header_df[file_to_header_df.zipped == 1]
    bad_zips = []
    for zip_path in tqdm(zip_files.raw_filepath, desc="Checking zips"):
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                n = len(zip_ref.namelist())
                if n != 1:
                    print(f"WARNING multi-file zip ({n} files): {os.path.basename(zip_path)}")
                    bad_zips.append(zip_path)
            
        except zipfile.BadZipFile:
            print(f"WARNING not a real zip: {os.path.basename(zip_path)}")
            bad_zips.append(zip_path)
    if bad_zips:
        print(f"\n{len(bad_zips)} problematic .zip file(s) — add to broken_files in Cell 8 if needed")

    # Append new classifications to existing table
    rawfiles_to_header = pd.concat([rawfiles_to_header, file_to_header_df], axis=0).drop_duplicates(subset=["raw_filepath"], keep="last").reset_index(drop=True)
    print(f"\nTotal classified files: {len(rawfiles_to_header)}")

else:
    print("No new files to classify.")


# COMMAND ----------

# DBTITLE 1,Add broken files with header=broken
# Add broken files to the classification table with header = "broken"
# This includes: (1) pre-known broken files from Cell 8, (2) newly discovered skipped files from Cell 11

# Resolve known broken_files basenames to full paths
known_broken_fullpaths = [f for f in all_raw_filepaths if os.path.basename(f) in broken_files]
known_broken_records = []
for f in known_broken_fullpaths:
    archive_format = detect_format(f)
    known_broken_records.append({
        "raw_filepath": f,
        "header": "broken",
        "source_period": "since2020",
        "zipped": int(archive_format == "zip" or f.endswith('.zip')),
        "file_format": "csv",
        "archive_format": archive_format if archive_format != "unknown" else None,
        "delimiter": None,
        "encoding": None,
        "encoding_source": "known_broken_list",
        "transform_format": None,
        "inner_file_name": None,
        "header_row": None,
        "normalized_header": None,
        "classification_status": "broken",
        "detection_notes": "File is listed in broken_files and excluded from header classification.",
        "detected_at": pd.Timestamp.utcnow(),
        "ingested_at": pd.NaT
    })

# Combine with any newly skipped files from Cell 11
broken_df = pd.DataFrame(known_broken_records)
if 'skipped_metadata' in dir() and skipped_metadata:
    broken_df = pd.concat([broken_df, pd.DataFrame(skipped_metadata)], axis=0, ignore_index=True)

# Remove any that are already in the table (in case of re-runs)
already_recorded = set(rawfiles_to_header.raw_filepath)
if not broken_df.empty:
    broken_df = broken_df[~broken_df.raw_filepath.isin(already_recorded)].drop_duplicates(subset=["raw_filepath"], keep="last")

if not broken_df.empty:
    rawfiles_to_header = pd.concat([rawfiles_to_header, broken_df], axis=0).drop_duplicates(subset=["raw_filepath"], keep="last").reset_index(drop=True)
    print(f"Added {len(broken_df)} broken files to classification table")
    for f in broken_df.raw_filepath:
        print(f"  - {os.path.basename(f)}")
else:
    print("No broken files to add (already recorded or none found)")

print(f"\nTotal records (classified + broken): {len(rawfiles_to_header)}")

# COMMAND ----------

# DBTITLE 1,Cell 12
# Save classification to Delta table
final_columns = [
    "raw_filepath",
    "header",
    "source_period",
    "zipped",
    "file_format",
    "archive_format",
    "delimiter",
    "encoding",
    "encoding_source",
    "transform_format",
    "inner_file_name",
    "header_row",
    "normalized_header",
    "classification_status",
    "detection_notes",
    "detected_at",
    "ingested_at"
]

for col in final_columns:
    if col not in rawfiles_to_header.columns:
        rawfiles_to_header[col] = None

rawfiles_to_header["detected_at"] = pd.to_datetime(rawfiles_to_header["detected_at"], utc = True, errors="coerce")
rawfiles_to_header["ingested_at"] = pd.to_datetime(rawfiles_to_header["ingested_at"], utc = True, errors="coerce")

rawfiles_to_header_spark = spark.createDataFrame(rawfiles_to_header[final_columns])
# Guard: insertInto is position-based — assert column order matches the table schema
# before writing so a future schema change fails loudly rather than silently corrupting data.
table_cols = [f.name for f in spark.table("prd_mega.scolom15.file_classification_since2020").schema.fields]
df_cols    = rawfiles_to_header_spark.columns
assert table_cols == df_cols, (
    f"Column order mismatch — fix final_columns to match the table schema before retrying.\n"
    f"  Table : {table_cols}\n"
    f"  DataFrame: {df_cols}"
)

# insertInto overwrites data only — schema and column comments are preserved
rawfiles_to_header_spark.write.insertInto("prd_mega.scolom15.file_classification_since2020", overwrite=True)

print(f"Saved {rawfiles_to_header_spark.count()} rows")
