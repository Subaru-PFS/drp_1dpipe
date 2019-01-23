import sys
import os
from setuptools import setup, find_packages

__minimum_python_version__ = '3.5.2'
if sys.version_info < tuple((int(val) for val in __minimum_python_version__.split('.'))):
    sys.stderr.write("ERROR: drp_1dpipe requires Python {} or later\n".format(
        __minimum_python_version__))
    sys.exit(1)

NAME = 'drp_1dpipe'
VERSION = '0.9.0'

# Add any necessary entry points
entry_points = {}
# Command-line scripts
entry_points['console_scripts'] = [
    'pre_process = drp_1dpipe.pre_process.pre_process:main',
    'process_spectra = drp_1dpipe.process_spectra.process_spectra:main',
    'merge_results = drp_1dpipe.merge_results.merge_results:main',
    'drp_1dpipe = drp_1dpipe.scheduler.scheduler:main'
]

setup(name=NAME,
      version=VERSION,
      description='PFS data-reduction pipeline',
      long_description=open(os.path.join(os.path.dirname(__file__),
                                         'README.md')).read(),
      url='https://pfs.ipmu.jp/',
      author='The LAM PFS-DRP1D developers',
      license='GPLv3',
      packages=find_packages(),
      requires=['numpy'],
      setup_requires=[],
      install_requires=[],
      classifiers=[
          "Development Status :: 1 - Planning",
          'Intended Audience :: Science/Research',
          'Operating System :: OS Independent',
          'Programming Language :: Python',
          'License :: OSI Approved :: GNU General Public License v3 or later '
          '(GPLv3+)',
          'Topic :: Scientific/Engineering :: Astronomy',
          'Topic :: Scientific/Engineering :: Physics'
      ],
      include_package_data=True,
      package_data={
         'drp_1dpipe': ['io/conf/*', 'io/auxdir/*'],
         },
      entry_points=entry_points,
      python_requires='>=' + __minimum_python_version__,
      tests_require=['pytest']
)
