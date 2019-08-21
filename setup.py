from setuptools import setup
from src.version import __version__

setup(
    name="runjob",
    version=__version__,
    packages=["runjob"],
    package_dir={"runjob": "src"},
    data_files=[("doc", ["doc/jobA.log", "doc/jobB.log", "doc/example.job"]), ],
    license="BSD",
    classifiers=[
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
    ],
    entry_points={
        'console_scripts': [
            'runjob = runjob.main:main'
        ]
    }
)
