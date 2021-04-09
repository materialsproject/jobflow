from pathlib import Path

from setuptools import find_packages, setup

module_dir = Path(__file__).resolve().parent

with open(module_dir / "README.md") as f:
    long_desc = f.read()

setup(
    name="activities",
    description="",
    long_description=long_desc,
    use_scm_version=True,
    setup_requires=["setuptools_scm"],
    long_description_content_type="text/markdown",
    url="https://github.com/hackingmaterials/activities",
    author="Alex Ganose",
    author_email="alexganose@googlemail.com",
    license="modified BSD",
    packages=find_packages("src"),
    package_dir={"": "src"},
    package_data={"activities": ["py.typed"]},
    zip_safe=False,
    include_package_data=True,
    install_requires=["setuptools", "monty", "pydash", "networkx", "maggma", "pydantic"],
    extras_require={
        "docs": [
            "sphinx==3.5.3",
            "furo==2021.3.20b30",
            "m2r2==0.2.7",
        ],
        "tests": ["pytest"],
        "fireworks": ["fireworks"],
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Science/Research",
        "Intended Audience :: System Administrators",
        "Intended Audience :: Information Technology",
        "Operating System :: OS Independent",
        "Topic :: Other/Nonlisted Topic",
        "Topic :: Database :: Front-Ends",
        "Topic :: Scientific/Engineering",
    ],
    python_requires=">=3.7",
    tests_require=["pytest"],
)
