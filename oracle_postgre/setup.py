import setuptools

setuptools.setup(
    name="oracle_postgre",
    packages=setuptools.find_packages(exclude=["oracle_postgre_tests"]),
    install_requires=[
        "dagster==0.15.5",
        "dagit==0.15.5",
        "pytest",
    ],
)
