import os
import sys
import sysconfig
import subprocess
import importlib.util

from setuptools import setup
from functools import partial, wraps
from setuptools.extension import Extension


class Packages(object):

    def __init__(self, pkg_name=""):
        self.name = pkg_name
        self.base_dir = os.path.dirname(__file__)
        basejoin = partial(self._join, self.base_dir)
        self.source_dir = basejoin("src")
        self.version_file = basejoin("src/_version.py")
        self.des_file = basejoin("README.md")
        self.req_file = basejoin("requirements.txt")

    def _join(self, *args):
        return os.path.join(*args)

    @property
    def listdir(self):
        df, nc = [], []
        if os.path.isdir(self.name):
            nc = os.listdir(self.name)
        for a, b, c in os.walk(self.source_dir):
            if os.path.basename(a).startswith("__"):
                continue
            for i in c:
                if i.startswith("__") or not i.endswith(".py") or i in nc:
                    continue
                p = os.path.join(a[len(self.source_dir)+1:], i)
                df.append(p)
        return df

    @property
    def description(self):
        des = ""
        if os.path.isfile(self.des_file):
            with open(self.des_file) as fi:
                des = fi.read()
        return des

    @property
    def version(self):
        for a, b, c in os.walk(self.source_dir):
            for i in c:
                f = os.path.join(a, i)
                if f.endswith("version.py"):
                    try:
                        v = load_module_from_path(f)
                        break
                    except:
                        continue
            else:
                continue
            break
        else:
            return "unknow"
        return v.__version__

    @property
    def git_version(self):
        git_hash = ''
        try:
            p = subprocess.Popen(
                ['git', 'log', '-1', '--format="%H %aI"'],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=os.path.dirname(__file__),
            )
        except FileNotFoundError:
            pass
        else:
            out, err = p.communicate()
            if p.returncode == 0:
                git_hash, git_date = (
                    out.decode('utf-8')
                    .strip()
                    .replace('"', '')
                    .split('T')[0]
                    .replace('-', '')
                    .split()
                )
        return git_hash

    @property
    def requirements(self):
        requires = []
        if os.path.isfile(self.req_file):
            with open(self.req_file) as fi:
                for line in fi:
                    line = line.strip()
                    requires.append(line)
        return requires

    @property
    def _extensions(self):
        exts = []
        for f in self.listdir:
            e = Extension(self.name + "." + os.path.splitext(f)[0].replace("/", "."),
                          [os.path.join(self.source_dir, f), ], extra_compile_args=["-O3", "-Wall", "-std=c99"],)
            e.cython_directives = {
                'language_level': sysconfig._PY_VERSION_SHORT_NO_DOT[0]}
            exts.append(e)
        return exts

    @property
    def _package_dir(self):
        pd = {}
        for a, b, v in os.walk(self.source_dir):
            p = a.replace(self.source_dir, self.name).replace("/", ".")
            pd[p] = a.replace(self.source_dir, "src")
            for d in b:
                pkg = os.path.join(a, d).replace(
                    "src", "").replace("/", ".").strip(".")
                pd[pkg] = os.path.join(a, d)
        return pd

    def install(self, ext=False):
        kwargs = dict(
            name=self.name,
            version=self.version,
            license="MIT",
            packages=list(self._package_dir.keys()),
            package_dir=self._package_dir,
            package_data={self.name: ["*.ini", "*.json"], },
            install_requires=self.requirements,
            python_requires='>=3.7',
            long_description=self.description,
            long_description_content_type='text/markdown',
            entry_points={'console_scripts': self._entrys},
            author="yodeng",
            author_email="yodeng@tju.edu.cn",
            url="https://github.com/yodeng/runjob",
        )
        if ext:
            kwargs.pop("package_dir")
            kwargs["ext_modules"] = self._extensions
        setup(**kwargs)

    @property
    def _entrys(self):
        eps = [f"{cmd} = {self.name}.main:entry_exec" for cmd in (
            "runflow", "runjob", "runsge", "runshell", "runbatch")] + \
            [f'qs = {self.name}.jobstat:main',
             f'qslurm = {self.name}.jobstat:qslurm',
             f'qcs = {self.name}.jobstat:batchStat'] + \
            [f"{self.name}-{cmd} = {self.name}._jobsocket:job_{cmd}" for cmd in (
                "server", "client", "report")]
        return eps


def dont_write_bytecode(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        ori = sys.dont_write_bytecode
        try:
            sys.dont_write_bytecode = True
            return func(*args, **kwargs)
        finally:
            sys.dont_write_bytecode = ori
    return wrapper


@dont_write_bytecode
def load_module_from_path(path):
    path = os.path.abspath(path)
    _, filename = os.path.split(path)
    module_name, _ = os.path.splitext(filename)
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def main():
    pkgs = Packages("runjob")
    pkgs.install(ext=0)


if __name__ == "__main__":
    main()
