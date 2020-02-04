import re
import subprocess
import sys
from pathlib import PurePath

from setuptools import find_packages, setup
from setuptools.command.develop import develop
from setuptools.command.install import install

if sys.version_info < (3, 5):
    raise Exception("avrokafka requires Python 3.5 or higher.")


__version__ = "1.0.0"
__author__ = "Dhia Abbassi"
__email__ = "dhia.absi@gmail.com"
__license__ = "Apache License 2.0"

packages = find_packages(exclude=["examples"])
base_dir = PurePath(__file__).parent

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


with open(PurePath(base_dir, "README.md")) as f:
    long_description = f.read()

classifiers = [
    "License :: OSI Approved :: Apache Software License",
    "Development Status :: 4 - Beta",
    "Programming Language :: Python :: 3 :: Only",
    "Programming Language :: Python :: 3.6",
    "Programming Language :: Python :: 3.7",
    "Programming Language :: Python :: 3.8",
    "Operating System :: OS Independent",
    "Intended Audience :: Developers",
    "Natural Language :: English",
    "Topic :: Software Development :: Libraries :: Python Modules",
]

setup(
    name="avrokafka",
    version=__version__,
    author=__author__,
    author_email=__email__,
    license=__license__,
    url="https://github.com/DhiaTN/avrokafka-py",
    description="A schema-registry aware avro serde (serializer/deserializer) for Apache Kafka",
    long_description="\n" + long_description,
    packages=packages,
    setup_requires=["pipenv"],
    cmdclass={"develop": PostDevelopCommand, "install": PostInstallCommand},
    zip_safe=False,
    keywords="avro,kafka,confluent,schema registry",
    classifiers=classifiers,
)
