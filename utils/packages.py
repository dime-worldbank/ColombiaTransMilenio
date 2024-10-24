
## to test if the import worked

def import_test_packages(x):
    print(x)




## import_packages.py

### files
import csv
from glob import iglob
from tqdm import tqdm
import rarfile
from pathlib import Path
from shutil import rmtree
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
from itertools import compress
import numpy as np
import pandas as pd

pd.options.display.float_format = '{:,.0f}'.format
# pd.set_option("display.max_rows", 100)
pd.options.display.max_columns = None
import datetime as dt
from random import sample, seed
seed(510)
# timezone = dt.timezone(offset = -dt.timedelta(hours=5), name = "America/Bogota")
timezone = dt.timezone(offset = -dt.timedelta(hours=0), name = "America/Bogota")
import re
#import fiona
import geopandas as gpd
import copy
from collections import Counter
import skmob
from skmob.preprocessing import clustering

### plotting
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import chart_studio.plotly as py
import plotly.graph_objs as go
import seaborn as sns
import folium
from folium.plugins import HeatMap, DualMap, Fullscreen
from folium.features import DivIcon
from branca.element import Template, MacroElement
import locale
from matplotlib.ticker import FuncFormatter
import matplotlib.lines as mlines
font = {'family' : 'Calibri',
        'weight' : 'normal',
        'size'   : 18}

import matplotlib

### jupyter
from IPython.display import HTML
import warnings
warnings.filterwarnings('ignore')
from IPython.display import display, HTML

display(HTML(data="""
<style>
    div#notebook-container    { width: 95%; }
    div#menubar-container     { width: 65%; }
    div#maintoolbar-container { width: 99%; }
</style>
"""))
