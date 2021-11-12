from setuptools import setup

with open("README.md", "r") as fh:
    readme = fh.read()

setup(name='Maceio',
      version='0.0.13',
      url='https://github.com/yuca-live/maceio',
      license='MIT License',
      author='Yuca Live',
      long_description=readme,
      long_description_content_type="text/markdown",
      author_email='time-data@gmail.com',
      keywords='Package, SQL, Json',
      description=u'Package to insert JSON data in SQL database.',
      packages=['Maceio'],
      install_requires=['sqlalchemy'],)
