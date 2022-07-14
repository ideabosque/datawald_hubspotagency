#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = "bibow"

from setuptools import find_packages, setup

setup(
    name="DataWald-HubspotAgency",
    license="MIT",
    version="0.0.1",
    author="Idea Bosque",
    author_email="ideabosque@gmail.com",
    description="DataWald HubspotAgency",
    long_description=__doc__,
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    platforms="Linux",
    install_requires=["DataWald-Agency", "DataWald-Connector", "Hubspot-Connector"],
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python",
        "Environment :: Web Environment",
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "Topic :: Internet :: WWW/HTTP :: Dynamic Content",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
)
