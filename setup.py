from pathlib import Path

from setuptools import find_packages, setup

module_dir = Path(__file__).resolve().parent

with open(module_dir / "README.md") as f:
    long_desc = f.read()

setup(
    name="jobflow",
    description="jobflow is library for writing computational workflows",
    long_description=long_desc,
    use_scm_version={"version_scheme": "python-simplified-semver"},
    setup_requires=["setuptools_scm"],
    long_description_content_type="text/markdown",
    url="https://materialsproject.github.io/jobflow",
    author="Alex Ganose",
    author_email="alexganose@googlemail.com",
    license="modified BSD",
    packages=find_packages("src"),
    package_dir={"": "src"},
    package_data={"jobflow": ["py.typed"]},
    zip_safe=False,
    include_package_data=True,
    install_requires=[
        "setuptools",
        "monty>=2021.5.9",
        "pydash",
        "networkx",
        "maggma>=0.38.1",
        "pydantic",
        "PyYAML",
    ],
    extras_require={
        "docs": [
            "sphinx==5.3.0",
            "furo==2022.9.29",
            "m2r2==0.3.3",
            "ipython==8.7.0",
            "nbsphinx==0.8.10",
            "nbsphinx-link==1.3.0",
            "FireWorks==2.0.3",
            "autodoc_pydantic==1.8.0",
        ],
        "tests": [
            "pytest==7.2.0",
            "pytest-cov==4.0.0",
            "FireWorks==2.0.3",
            "matplotlib==3.6.2",
            "pydot==1.4.2",
            "moto==4.0.9",
        ],
        "dev": ["pre-commit>=2.12.1"],
        "vis": ["matplotlib", "pydot"],
        "fireworks": ["fireworks"],
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Science/Research",
        "Intended Audience :: System Administrators",
        "Intended Audience :: Information Technology",
        "Operating System :: OS Independent",
        "Topic :: Other/Nonlisted Topic",
        "Topic :: Database :: Front-Ends",
        "Topic :: Scientific/Engineering",
    ],
    python_requires=">=3.8",
    tests_require=["pytest"],
)
