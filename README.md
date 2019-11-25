runjob
========================

Summary
=======

runjob is a program for managing a group of related jobs running on a
compute cluster.  It provides a convenient method for specifying
dependencies between jobs and the resource requirements for each job
(e.g. memory, CPU cores). It monitors the status of the jobs so you
can tell when the whole group is done. Litter cpu or memory resource
is used in the login compute node.

Software Requirements
=====================

python 2.7

Installation
============

	git clone https://github.com/yodeng/runjob.git

	pip install ./runjob

	or:

	pip install runjob

User Guide
======
All manual instruction for runjob can be found in [PDF中文文档使用说明](https://github.com/yodeng/runjob/blob/master/doc/manual_instruction_for_runjob.pdf)

Usage
=====

You can run a quick test like this:

	$ runjob doc/example.job
    
	$ runstate doc/example.job

	$ runsge --help

License
=======

runjob is distributed under the BSD 3-clause licence.  

Contact
=======

Please send comments, suggestions, bug reports and bug fixes to
yodeng@tju.edu.cn.

Todo
=======

More functions will be improved in the future.

