import os

from setuptools import setup
from src.version import __version__


def getdes():
    des = ""
    with open(os.path.join(os.getcwd(), "README.md")) as fi:
        des = fi.read()
    return des


def listdir(path):
    df = []
    for a, b, c in os.walk(path):
        df.append((a, [os.path.join(a, i) for i in c]))
    return df


setup(
    name="runjob",
    version=__version__,
    packages=["runjob"],
    package_data={"runjob": ["*.ini", "*.json"]},
    package_dir={"runjob": "src"},
    data_files=listdir("doc"),
    author="Deng Yong",
    author_email="yodeng@tju.edu.cn",
    url="https://github.com/yodeng/runjob",
    license="BSD",
    install_requires=["psutil", "ratelimiter", "batchcompute"],
    python_requires='>=2.7.10, <3.10',
    long_description=getdes(),
    long_description_content_type='text/markdown',
    entry_points={
        'console_scripts': [
            'runjob = runjob.run:main',
            'qs = runjob.jobstat:main',
            'qcs = runjob.jobstat:batchStat',
            'runsge = runjob.sge_run:main',
            'runbatch = runjob.sge_run:main',
            'runsge0 = runjob.sge:main',
        ]
    }
)
