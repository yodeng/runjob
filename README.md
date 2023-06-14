# runjob

[![OSCS Status](https://www.oscs1024.com/platform/badge/yodeng/runjob.svg?size=small)](https://www.oscs1024.com/project/yodeng/runjob?ref=badge_small)
[![PyPI version](https://img.shields.io/pypi/v/runjob.svg?logo=pypi&logoColor=FFE873)](https://pypi.python.org/pypi/runjob)
[![Downloads](https://pepy.tech/badge/runjob)](https://pepy.tech/project/runjob)
[![install with bioconda](https://img.shields.io/badge/install%20with-bioconda-brightgreen.svg?style=flat)](https://anaconda.org/bioconda/runjob)

## Summary

runjob is a program for managing a group of related jobs running on a compute cluster `localhost`, [Sun Grid Engine](http://star.mit.edu/cluster/docs/0.93.3/guides/sge.html), [BatchCompute](https://help.aliyun.com/product/27992.html) .  It provides a convenient method for specifying dependencies between jobs and the resource requirements for each job (e.g. memory, CPU cores). It monitors the status of the jobs so you can tell when the whole group is done. Litter cpu or memory resource is used in the login compute node.

## OSCS

[![OSCS Status](https://www.oscs1024.com/platform/badge/yodeng/runjob.svg?size=large)](https://www.oscs1024.com/project/yodeng/runjob?ref=badge_large)

## Software Requirements

python >=2.7.10, <3.11

## Installation

The development version can be installed with (for recommend)

```
pip install git+https://github.com/yodeng/runjob.git
```

The stable release (maybe not latest) can be installed with

> pypi:

```
pip install runjob -U
```

> conda:

```
conda install -c bioconda runjob
```

## User Guide

All manual can be found [here](https://runjob.readthedocs.io/en/latest/).

## Usage

You can run a quick test like this:

	$ runjob -j doc/example.job
	
	$ qs doc/example.job
	
	$ qcs --help
	
	$ runsge/runshell/runbatch --help

## License

runjob is distributed under the [MIT license](./LICENSE).

## Contact

Please send comments, suggestions, bug reports and bug fixes to
yodeng@tju.edu.cn.

## Todo

More functions will be improved in the future.
