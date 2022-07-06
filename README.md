# als.bcs

Extract data and metadata from text data files that were created by
the Beamline Controls System (BCS) at the Advanced Light Source (ALS)

Purpose
---
Facilitate the extraction of header information and scan input variables
from common BCS data file formats, such as:

* Time Scan
* Single Motor Scan
* Trajectory Scan

### bcs.find module

Find BCS data files that match provided criteria 
(date, scan type, scan nmuber, etc.) within their native directory structure.

### bcs.data module

Read scan output information and data from selected BCS data files.

### bcs.scan module

Find and read scan input information BCS Trajectory Scan data files.

### bcs.ingest module

Read header information from selected BCS data files.

Installation
---

### Using pip

```bash
# PyPI
python -m pip install als.bcs

# -- OR --

# Local copy of the project repository
python -m pip install -r requirements.txt -e .

# -- OR --

# Local tarball, uses static version file generated during build
python -m pip install als.bcs-0.2.0.tar.gz
```


Copyright Notice
---
als.bcs: Extract metadata from text data files that were created by
the Beamline Controls System (BCS) at the Advanced Light Source (ALS), 
Copyright (c) 2022, The Regents of the University of California, through 
Lawrence Berkeley National Laboratory (subject to receipt of any required 
approvals from the U.S. Dept. of Energy). All rights reserved.

If you have questions about your rights to use or distribute this software, 
please contact Berkeley Lab's Intellectual Property Office at IPO@lbl.gov.

NOTICE. This Software was developed under funding from the U.S. Department of 
Energy and the U.S. Government consequently retains certain rights. As such, 
the U.S. Government has been granted for itself and others acting on its 
behalf a paid-up, nonexclusive, irrevocable, worldwide license in the 
Software to reproduce, distribute copies to the public, prepare derivative 
works, and perform publicly and display publicly, and to permit other to do 
so. 
