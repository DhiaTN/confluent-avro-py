import subprocess
import sys
from pathlib import Path

from setuptools import find_packages, setup
from setuptools.command.develop import develop
from setuptools.command.install import install

if sys.version_info < (3, 6):
    raise Exception("confluent_avro requires Python 3.6 or higher.")


__version__ = "1.3.0"
__author__ = "Dhia Abbassi"
__email__ = "dhia.absi@gmail.com"
__license__ = "Apache License 2.0"

base_dir = Path(__file__).parent

pipenv_command = ["pipenv", "install", "--deploy", "--system"]
pipenv_command_dev = ["pipenv", "install", "--dev", "--deploy", "--system"]


class PostDevelopCommand(develop):
    """Post-installation for development mode."""

    def run(self):
        subprocess.check_call(pipenv_command_dev)
        develop.run(self)


class PostInstallCommand(install):
    """Post-installation for installation mode."""

    def run(self):
        subprocess.check_call(pipenv_command)
        install.run(self)


README = (base_dir / "README.md").read_text()
packages = find_packages(exclude=["examples", "tests"])
classifiers = [
    "License :: OSI Approved :: Apache Software License",
    "Development Status :: 4 - Beta",
    "Programming Language :: Python :: 3 :: Only",
    "Programming Language :: Python :: 3.6",
    "Programming Language :: Python :: 3.7",
    "Operating System :: OS Independent",
    "Intended Audience :: Developers",
    "Natural Language :: English",
    "Topic :: Software Development :: Libraries :: Python Modules",
]

setup(
    name="confluent-avro",
    version=__version__,
    author=__author__,
    author_email=__email__,
    license=__license__,
    url="https://github.com/DhiaTN/confluent-avro-py",
    description="An Avro SerDe implementation that integrates with the confluent schema registry and serializes and deserializes data according to the defined confluent wire format",
    long_description=README,
    long_description_content_type="text/markdown",
    packages=packages,
    setup_requires=["pipenv"],
    cmdclass={"develop": PostDevelopCommand, "install": PostInstallCommand},
    zip_safe=False,
    keywords="avro,kafka,confluent,schema registry",
    classifiers=classifiers,
)
