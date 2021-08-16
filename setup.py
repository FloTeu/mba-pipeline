from setuptools import find_packages, setup

# with open('requirements.txt') as fp:
#    install_requires = fp.read()

# INSTALL_REQUIRES = [
#     'SQLAlchemy',
#     'pandas',
#     'opencv-python-headless',
#     'webcolors',
#     'Pillow',
#     'sklearn',
#     'regex',
#     'httplib2',
#     'matplotlib',
#     'seaborn',
#     'PyYAML',
#     'requests',
#     'gitpython',
#     'google-cloud-bigquery',
#     'google-cloud-storage'
#     ]

setup(name='mwfunctions',
      version='0.1',
      description='Codebase for Merchwatch Backend',
      url='https://merchwatch.net/',
      author='Merchwatch',
      author_email='contact@merchwatch.net',
      install_requires=None, #  INSTALL_REQUIRES,
      license='TODO',
      packages=find_packages(),
      zip_safe=False)
