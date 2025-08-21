"""
Setup script for HRRR Ingest package.
"""

from setuptools import setup, find_packages
from pathlib import Path

# Read the README file
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

# Read requirements
requirements = []
with open("requirements.txt", "r") as f:
    for line in f:
        line = line.strip()
        if line and not line.startswith("#"):
            requirements.append(line)

setup(
    name="hrrr-ingest",
    version="0.1.0",
    author="HRRR Ingest Team",
    author_email="team@hrrr-ingest.com",
    description="A CLI tool for downloading and processing HRRR forecast data",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/your-org/hrrr-ingest",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Scientific/Engineering :: Atmospheric Science",
        "Topic :: Scientific/Engineering :: Information Analysis",
    ],
    python_requires=">=3.8",
    install_requires=requirements,
    entry_points={
        "console_scripts": [
            "hrrr-ingest=hrrr_ingest.cli:main",
        ],
    },
    include_package_data=True,
    zip_safe=False,
    keywords="weather forecast hrrr grib meteorology climate",
    project_urls={
        "Bug Reports": "https://github.com/your-org/hrrr-ingest/issues",
        "Source": "https://github.com/your-org/hrrr-ingest",
        "Documentation": "https://github.com/your-org/hrrr-ingest/blob/main/README.md",
    },
)

