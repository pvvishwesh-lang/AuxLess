from setuptools import setup, find_packages

setup(
    name="auxless",
    version="0.2.0",
    packages=find_packages(),
    install_requires=[
        "google-cloud-firestore",
        "google-cloud-storage",
        "google-cloud-pubsub",
        "requests",
        "pandas",
        "numpy",
        "scipy",
    ],
)
