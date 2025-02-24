from setuptools import find_packages, setup

setup(
    name="dagster_incremental",
    packages=find_packages(exclude=["dagster_incremental_tests"]),
    install_requires=[
        "dagster==1.9.*",
        "dagster-cloud",
        "dagster-duckdb",
        "geopandas",
        "kaleido",
        "pandas",
        "plotly",
        "shapely",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
