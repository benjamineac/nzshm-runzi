#build_rupture_set_index

"""
Simple script to creawte valid URLs to the rupture sets built

only to be used until we have automated rupture reporting

"""

import os
import shutil
import fnmatch
from pathlib import PurePath

dir = '/home/chrisbc/DEV/GNS/opensha-new/DATA'
patterns = ['index.html', '*.zip']

for root, dirs, files in os.walk(dir):
    for pattern in patterns:
        for filename in fnmatch.filter(files, pattern):
            print(PurePath(root.replace(dir, ''), filename))
