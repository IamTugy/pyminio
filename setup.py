import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pyminio",
    version="0.1.0",
    author="Michael Tugendhaft",
    author_email="tugmica@gmail.com",
    description="Python client for Minio",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/mmm1513/pyminio",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)