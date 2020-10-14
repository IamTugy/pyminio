import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pyminio",
    version="0.2.1",
    author="Michael Tugendhaft",
    author_email="tugmica@gmail.com",
    description="Python client for Minio",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/mmm1513/pyminio",
    packages=setuptools.find_packages(),
    install_requires=[
        'minio <7.0.0, >=6.0.0',
        'pytz',
        'cached-property',
        'attrdict',
        'dataclasses'
    ],
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8'
    ],
    python_requires='>=3.6',
)
