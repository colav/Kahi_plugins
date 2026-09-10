#!/usr/bin/env python3
"""Package configuration for the shared ImpactU type catalog."""

from pathlib import Path

from setuptools import find_packages, setup


ROOT = Path(__file__).resolve().parent


def get_version():
    namespace = {}
    version_file = ROOT / "kahi_impactu_type_catalog" / "_version.py"
    exec(version_file.read_text(encoding="utf-8"), namespace)
    return namespace["__version__"]


setup(
    name="Kahi_impactu_type_catalog",
    version=get_version(),
    author="CoLaV",
    author_email="colav@udea.edu.co",
    url="https://github.com/colav/Kahi_plugins",
    license="BSD-3-Clause",
    description="Shared, versioned ImpactU type routing catalog",
    long_description=(ROOT / "README.md").read_text(encoding="utf-8"),
    long_description_content_type="text/markdown",
    packages=find_packages(exclude=["tests"]),
    include_package_data=True,
    package_data={
        "kahi_impactu_type_catalog.data": ["*.json", "*.xlsx"],
    },
    install_requires=[],
    extras_require={"build": ["openpyxl"]},
    python_requires=">=3.8",
)
