#!/usr/bin/env python3
# coding: utf-8

from __future__ import print_function

import codecs
import os
import sys

from setuptools import find_packages, setup


v = sys.version_info


def read(rel_path):
    here = os.path.abspath(os.path.dirname(__file__))
    with codecs.open(os.path.join(here, rel_path), "r") as fp:
        return fp.read()


def get_version(rel_path):
    for line in read(rel_path).splitlines():
        if line.startswith("__version__"):
            delim = '"' if '"' in line else "'"
            return line.split(delim)[1]
    raise RuntimeError("Unable to find version string.")


shell = False
if os.name in ("nt", "dos"):
    shell = True
    warning = "WARNING: Windows is not officially supported"
    print(warning, file=sys.stderr)


def main():
    setup(
        name="Kahi_snies_affiliations",
        version=get_version("kahi_snies_affiliations/_version.py"),
        author="Colav",
        author_email="colav@udea.edu.co",
        packages=find_packages(exclude=["tests"]),
        include_package_data=True,
        url="https://github.com/colav/Kahi_plugins",
        license="BSD",
        description="Kahi plugin for processing affiliations from SNIES institutions",
        long_description=open("README.md").read(),
        long_description_content_type="text/markdown",
        install_requires=[
            "kahi",
            "pymongo",
            "joblib",
            "thefuzz",
            "unidecode",
        ],
    )


if __name__ == "__main__":
    main()
