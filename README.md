# PySpark transformations

## Overview
This project loads the provided CSV datasets, apply the specified Spark transformations, and save the result as a single CSV file named.


## Project setup

### 1. Create a Virtual environment
This project uses `.venv` as the default virtual environment folder.

> CLI setup can be done by desired IDE like [PyCharm](https://www.jetbrains.com/help/pycharm/creating-virtual-environment.html)
```shell
# Create venv in a specific folder (for this project in the root folder: /pyspark-transformations)
python3 -m venv myvenv

# Activate venv
source venv/bin/activate
```

### 2. Install dependencies
Project dependencies are managed by [UV - Python packaging and dependency management tool](https://docs.astral.sh/uv/). \
Inside the already activated venv run CLI commands:

```shell
# Install UV with pip
pip install uv

# Install dependencies with UV
uv sync
```