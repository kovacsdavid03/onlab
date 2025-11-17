from setuptools import find_packages, setup

setup(
    name="movies_load_pkg01",
    packages=find_packages(exclude=["movies_load_pkg01_tests", "movies_incremental_load_pkg_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "pandas",
        "sqlalchemy",
        "pyodbc",
        "kaggle"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
