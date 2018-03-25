


import pandas as pd
import numpy as np
from pandas import DataFrame

# from pandas_datareader import data
from datetime import datetime, timedelta

from sqlalchemy import create_engine

import requests
import urllib

import re
import os
import logging
import time
import sys

# sys.path.append('..')

# import Crawler as cw


# ckeck_dir = "../BK20180323/StockInfo_otc/"
check_dir = "../StockInfo/"

print(sys.argv)

if len(sys.argv) == 2:
	check_dir = sys.argv[1]
	print("start " + check_dir)
	for file in os.listdir(check_dir):
	    if file.startswith('.'):
	        continue
	    if not file.endswith('.db'):
	    	continue

	    engine = create_engine('sqlite:///%s%s' %
	                         (check_dir, file), echo=False)
	    for table_name in engine.table_names():
	    	try:
	    		df = pd.read_sql(table_name, con=engine)
	    	except:
	    		print(file + " of " + table_name)

	print("done")
else:
	print('argv ERROR')