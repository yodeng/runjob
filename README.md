# runjob

[![PyPI version](https://img.shields.io/pypi/v/runjob.svg?logo=pypi&logoColor=FFE873)](https://pypi.python.org/pypi/runjob)
[![Downloads](https://pepy.tech/badge/runjob)](https://pepy.tech/project/runjob)
[![Anaconda-Server Badge](https://anaconda.org/yodeng/runjob/badges/version.svg)](https://anaconda.org/yodeng/runjob)
[![Anaconda-Server Badge](https://anaconda.org/yodeng/runjob/badges/installer/conda.svg)](https://conda.anaconda.org/yodeng)

## Summary

runjob is a program for managing a group of related jobs running on a compute cluster `localhost`, [Sun Grid Engine](http://star.mit.edu/cluster/docs/0.93.3/guides/sge.html), [BatchCompute](https://help.aliyun.com/product/27992.html) .  It provides a convenient method for specifying dependencies between jobs and the resource requirements for each job (e.g. memory, CPU cores). It monitors the status of the jobs so you can tell when the whole group is done. Litter cpu or memory resource is used in the login compute node.

## Software Requirements

python >=2.7.10, <=3.10

## Installation

The latest release can be installed with

> pypi:

```
pip install runjob
```

> conda:

```
conda install -c yodeng runjob
```

The development version can be installed with

```
pip install git+https://github.com/yodeng/runjob.git
```

## User Guide

All manual instruction for runjob can be found in [here](https://runjob.readthedocs.io/en/latest/).

## Usage

You can run a quick test like this:

	$ runjob -j doc/example.job
	
	$ qs doc/example.job
	
	$ qcs --help
	
	$ runsge/runbatch --help
	
	$ runsge0 --help

## License

runjob is distributed under the BSD 3-clause licence.  

## Contact

Please send comments, suggestions, bug reports and bug fixes to
yodeng@tju.edu.cn.

## Todo

More functions will be improved in the future.

