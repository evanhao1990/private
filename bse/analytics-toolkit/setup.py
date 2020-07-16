from setuptools import setup
import os
git_user = os.environ.get('GIT_USER')

setup(
     name='analytics-toolkit',  
     version='0.3',
     author="Rory Vigus",
     author_email="rory.vigus@bestseller.com",
     description="A collection of common analytics packages",
     url="https://{}/bestseller-ecom/analytics-toolkit.git".format(git_user),
     packages=['database_connector', 'plotting', 'bigquery_api', 'aws_ana', 'ana_images'],
     package_data={'ana_images':['templates/report.html']},
     zip_safe=False)