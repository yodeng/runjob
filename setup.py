import os

from setuptools import setup
from src.version import __version__


def getdes():
    des = ""
    with open(os.path.join(os.getcwd(), "README.md")) as fi:
        des = fi.read()
    return des


setup(
    name="runjob",
    version=__version__,
    packages=["runjob"],
    package_dir={"runjob": "src"},
    data_files=[("doc", [os.path.join("doc", i)
                         for i in os.listdir("doc")]), ],
    author="Deng Yong",
    author_email="yodeng@tju.edu.cn",
    url="https://github.com/yodeng/runjob",
    license="BSD",
    install_requires=["psutil"],
    classifiers=[
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
    ],
    long_description=getdes(),
    long_description_content_type='text/markdown',
    entry_points={
        'console_scripts': [
            'runjob = runjob.main:main',
            'runstate = runjob.stat:main',
            'runsge = runjob.sge:main',
        ]
    }
)
