#!/usr/bin/env python3
# coding: utf-8

# Copyright (c) Colav.
# Distributed under the terms of the Modified BSD License.

# -----------------------------------------------------------------------------
# Minimal Python version sanity check (from IPython)
# -----------------------------------------------------------------------------

# See https://stackoverflow.com/a/26737258/2268280
# sudo pip3 install twine
# python3 setup.py sdist bdist_wheel
# twine upload dist/*
# For test purposes
# twine upload --repository-url https://test.pypi.org/legacy/ dist/*

from __future__ import print_function
from setuptools import setup, find_packages

import os
import sys
import codecs


v = sys.version_info


def get_version(rel_path):
    for line in read(rel_path).splitlines():
        if line.startswith('__version__'):
            delim = '"' if '"' in line else "'"
            return line.split(delim)[1]
    else:
        raise RuntimeError("Unable to find version string.")


def read(rel_path):
    here = os.path.abspath(os.path.dirname(__file__))
    with codecs.open(os.path.join(here, rel_path), 'r') as fp:
        return fp.read()


shell = False
if os.name in ('nt', 'dos'):
    shell = True
    warning = "WARNING: Windows is not officially supported"
    print(warning, file=sys.stderr)


def main():
    setup(
        # Application name:
        name="Kahi_impactu",

        # Version number (initial):
        version=get_version('kahi_impactu/_version.py'),

        # Application author details:
        author="Colav",
        author_email="colav@udea.edu.co",

        # Packages
        packages=find_packages(exclude=['tests']),

        # Include additional files into the package
        include_package_data=True,

        # Details
        url="https://github.com/colav/Kahi_plugins",
        #
        license="BSD",

        description="Kahi impactu metapackage to install all plugins for impactu ETL",

        long_description=open("README.md").read(),

        long_description_content_type="text/markdown",

        # Dependent packages (distributions)
        # put you packages here
        install_requires=[
            'kahi==0.1.2a0',
            'Kahi_doaj_sources==0.1.1-beta',
            'Kahi_elasticsearch_works==0.1.0-beta',
            'Kahi_impactu_postcalculations==0.1.4',
            'Kahi_impactu_post_cites_count==0.0.4-alpha',
            'Kahi_minciencias_opendata_affiliations==0.1.5',
            'Kahi_minciencias_opendata_person==0.1.5-beta',
            'Kahi_minciencias_opendata_works==0.1.2',
            'Kahi_minciencias_opendata_works_misc == 0.1.0-beta',
            'Kahi_minciencias_opendata_projects==0.1.1-beta',
            'Kahi_minciencias_opendata_patents==0.1.1-beta',
            'Kahi_minciencias_opendata_events==0.1.1-beta',
            'Kahi_openalex_affiliations==0.1.3',
            'Kahi_openalex_person==0.1.5',
            'Kahi_openalex_sources==0.1.2-beta',
            'Kahi_openalex_subjects==0.1.1-beta',
            'Kahi_openalex_works==0.1.8',
            'Kahi_post_cleanup_entities==0.0.1-alpha',
            'Kahi_post_person_work_cleaning==0.1.0',
            'Kahi_ciarp_works==0.1.0-beta',
            'Kahi_ror_affiliations==0.1.4',
            'Kahi_scholar_person==0.1.0',
            'Kahi_scholar_works==0.1.5',
            'Kahi_scienti_affiliations==0.1.5',
            'Kahi_scienti_person==0.1.6',
            'Kahi_scienti_sources==0.1.2-beta',
            'Kahi_scienti_works==0.1.6',
            'Kahi_scimago_sources==0.1.0-beta',
            'Kahi_staff_affiliations==0.1.2-beta',
            'Kahi_staff_person==0.1.5',
            'Kahi_unicity_person==0.1.0',
            'Kahi_unicity_person_graph==0.1.1',
            'Kahi_wikidata_affiliations==0.1.1-beta',
            'Kahi_siiu_projects == 0.0.1-alpha'
        ],
    )


if __name__ == "__main__":
    main()
