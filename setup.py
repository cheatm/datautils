from setuptools import find_packages, setup


PACKAGES = find_packages()
REQUIRES = open("requirements.txt").read().split("\n")


setup(
    name="datautils",
    version="0.0.1",
    packages=PACKAGES,
    install_requires=REQUIRES,
    license="Apache License v2",
    author="Cam",
    author_email="cam@fxdayu.com"
)

