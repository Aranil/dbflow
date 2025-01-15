from setuptools import setup, find_packages

setup(
    name="dbflow",
    version="0.1.0",
    author="Aranil",
    author_email="linara.arslanova@uni-jena.de",
    description="Module to handle SQLite database operations and provide utilities",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/Aranil/dbflow",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 10",
        "License :: OSI Approved :: MIT License",
        "Operating System :: WIN 10",
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