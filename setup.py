from setuptools import find_packages, setup
import versioneer

setup(
    name="intake_meta_esm",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    author="Dougie Squire",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.9",
    install_requires=[
        "intake",
        "intake-esm",
    ],
    entry_points={
        "intake.drivers": [
            "meta_esm_datastore = intake_meta_esm.core:meta_esm_datastore",
        ]
    },
)
