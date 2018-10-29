import sys

__minimum_python_version__ = '3.5.2'
if sys.version_info < tuple((int(val) for val in __minimum_python_version__.split('.'))):
    sys.stderr.write("ERROR: drp_1dpipe requires Python {} or later\n".format(
        __minimum_python_version__))
    sys.exit(1)

import os
from setuptools import setup

import drp_1dpipe

NAME = 'drp_1dpipe' #PEP 508
VERSION = '0.0.0'   #PEP 440

# Add any necessary entry points
entry_points = {}
# Command-line scripts
entry_points['console_scripts'] = [
    'split = drp_1dpipe.split.split:main',
    'process_spectra = drp_1dpipe.process_spectra.process_spectra:main'
]

setup(name=NAME,
      version=VERSION,
      description='PFS data-reduction pipeline',
      long_description=open(os.path.join(os.path.dirname(__file__),
                            'README.md')).read(),
      url='https://pfs.ipmu.jp/',
      author='The LAM PFS-DRP1D developers',
      license='GPLv3', # QUESTION : what licence is used in PFS ?
      packages=['drp_1dpipe'],
      requires=['numpy'],
      setup_requires=[], # TODO
      install_requires=[], # TODO
      classifiers=[
          "Development Status :: 1 - Planning",
          'Intended Audience :: Science/Research',
          'Operating System :: OS Independent',
          'Programming Language :: Python',
          'License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)', # QUESTION : same as above
          'Topic :: Scientific/Engineering :: Astronomy',
          'Topic :: Scientific/Engineering :: Physics'
      ],
      entry_points=entry_points,
      python_requires='>=' + __minimum_python_version__,
      tests_require=['pytest']
)
