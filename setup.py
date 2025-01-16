from setuptools import setup, find_packages
from pathlib import Path


long_description = Path("README.md").read_text() if Path("README.md").exists() else ""


setup(
    name="dbflow",
    version="0.1.0",
    author="Aranil",
    author_email="linara.arslanova@uni-jena.de",
    description="Module to handle SQLite database operations and provide utilities",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Aranil/dbflow",
    packages=find_packages(),
    include_package_data=True,  # Include non-Python files specified in MANIFEST.in
    package_data={
        'dbflow.dbflow': ['sql/*.sql'],  # Include all .sql files in the sql folder
    },
    classifiers=[
        "Programming Language :: Python :: 3.10",
        "License :: OSI Approved :: MIT License",
        "Operating System :: Microsoft :: Windows",
    ],
    python_requires='>=3.10',
    install_requires=[
        "geopandas",
        #"python-decouple",
        #"rasterio",
        #"fiona",
        #"py7zr",
        #"scikit-image",
        #"opencv-python",
        #"seaborn",
        #"zarr",
        #"cartopy"
    ],
)

# run conda env create -f environment.yml to install required packages in conda env

# run pip install git+ssh://git@github.com/your-user/your-private-repo.git to install package