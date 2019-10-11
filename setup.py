from setuptools import setup

setup(
    name='bq-external-table',
    version='0.0.1',
    packages=["bq_external_table"],
    license='marionete',
    author='marionete',
    author_email='',
    description='This application deals with data migration between GCP buckets and BigQuery external tables.',
    install_requires=["google-cloud-storage==1.20.0", "google-cloud-bigquery==1.20.0"],
    entry_points={
        'console_scripts': [
            'bq-external-table = bq_external_table.main:main'
        ]
    },
    include_package_data=True
)
