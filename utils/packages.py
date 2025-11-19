# Databricks notebook source


!pip install rarfile
!pip install findspark
!pip install pyspark
!pip install plotly
!pip install pyspark_dist_explore
!pip install geopandas
!pip install seaborn
!pip install folium
!pip install editdistance
!pip install scikit-mobility
!pip install chart_studio
!pip install tqdm
!pip install pyunpack
!pip install patool
!pip install deltalake


## import_packages.py

### files
import shutil
import sys
import os
import csv
from glob import iglob
from tqdm import tqdm
import rarfile
from pathlib import Path
from pyunpack import Archive
import chardet
import zipfile

### spark etc
import findspark, os, pyspark, time, sys
import pyspark.sql.functions as F
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark_dist_explore import Histogram, hist, distplot, pandas_histogram
from pyspark import *
from pyspark.sql import *
from pyspark.rdd import *
from pyspark.ml import *
from pyspark.sql.types import ArrayType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import DoubleType
from pyspark.sql.types import FloatType
from delta.tables import *
import multiprocessing

### data wrangling
import itertools
import numpy as np
import pandas as pd

pd.options.display.float_format = '{:,.0f}'.format
import datetime as dt
from random import sample, seed
seed(510)
timezone = dt.timezone(offset = -dt.timedelta(hours=0), name = "America/Bogota")
import re
import geopandas as gpd
import copy
from collections import Counter
#import skmob
#from skmob.preprocessing import clustering

