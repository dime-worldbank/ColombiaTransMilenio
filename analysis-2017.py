# Databricks notebook source
# Modules
import os
from pathlib import Path
import shutil
from shutil import rmtree
import pandas as pd

!pip install tqdm
from tqdm import tqdm

!pip install pyunpack
!pip install patool
from pyunpack import Archive

!pip install rarfile

# Directories
path = '/mnt/DAP/data/ColombiaProject-TransMilenioRawData/'

# COMMAND ----------



# COMMAND ----------

raw2017_dir = path + '/Workspace/Raw/2017'
