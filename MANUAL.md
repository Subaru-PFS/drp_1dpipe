# DRP_1DPIPE - User's Manual

## Installation

See README.md

## Configuration

### Runtime environment

Pipeline is run with the `drp_1dpipe` command.

#### Synopsis
```
usage: drp_1dpipe [-h] [-v] [--workdir WORKDIR] [--logdir LOGDIR]
                  [--loglevel LOGLEVEL] [--scheduler {local,pbs,slurm}]
                  [--pre-commands COMMAND] [--spectra-dir DIR]
                  [--bunch-size SIZE] [--lineflux {on,off,only}]
                  [--parameters-file FILE] [--linemeas-parameters-file FILE]
                  [--concurrency CONCURRENCY] [--output-dir DIR]
                  [--config FILE]
```
#### Optional arguments

Arguments can be given on the command line or in a configuration file. Command line arguments take precedence.

##### `--workdir WORKDIR`

The root working directory where data is located.

##### `--logdir LOGDIR`

The logging directory.

##### `--loglevel LOGLEVEL`

The logging level. `CRITICAL`, `ERROR`, `WARNING`, `INFO` or `DEBUG`.

##### `--scheduler SCHEDULER`

The scheduler to use. Either `local` or `pbs`.

##### `--pre-commands COMMAND`

Commands to run before before `process_spectra`. This gives the opportunity for each process to initialize a virtualenv or mount a data directory.
`COMMAND` is a list of commands given as a python expression.

##### `--spectra-dir DIR`

Base path where to find spectra. Relative to workdir.

##### `--bunch-size SIZE, -n SIZE`

Maximum number of spectra per bunch.

##### `--linemeas LINEMEAS`

Activate line measurement. Either `on` (line measurement is executed after redshift determination), or `off` (no line measurement), or `only` (only line measurement, no redshift determination).

##### `--parameters-file FILE`

Path to parameter file for redshift determination.

##### `--linemeas-parameters-file FILE`

Path to parameter file for line measurement.

##### `--concurrency CONCURRENCY, -j CONCURRENCY`

Set concurrency level for parallel run. -1 means infinity (default: 1)

##### `--output-dir DIR, -o DIR`

The output directory (default: output)

##### `--config FILE`

Configuration file giving all these command line arguments.

##### `-h`, `--help`

Show the help message and exit.

#### Examples

Run using all defaults arguments values (see in table below for default values):

```sh
drp_1dpipe
```

Run on a PBS queue, activating a virtualenv on each node before running:

```sh
drp_1dpipe --scheduler pbs --pre-commands "source $HOME/venv/bin/activate"
```

Run bunches of 200 spectra on a multi-core machine using 4 cores and store results in the `my-output-dir` directory:

```sh
drp_1dpipe -n 200 -j 4 -o my-output-dir
```

### Algorithmic parameters

Algorithms are described in [DRP Algorith documentation](https://sumire.pbworks.com/w/file/132378141/LAM-PFS-1D-DRP-Algo-Pipeline_v0.82.pdf "DRP-Algo").

Redshift determination algorithms can be tuned using an optional JSON file.
An example file can be found in `drp_1dpipe/io/auxdir/parameters.example.json`.

Parameters are as follow :

**General parameters** : Parameters always applicable

| **Parameter name** | **Type** | **Default** | **Description** |
| - | - | - | - |
| `lambdarange` | `["min", "max"]` | `[ "3000", "13000"]` | lambda range in Angströms|
| `redshiftrange` | `["min", "max"]` | `[ "0.0", "6."]` | redshift range|
| `redshiftstep` | `float` | `0.0001` | redshift step for linear scale or lowest step for log scale|
| `redshiftsampling` |{`log`,`lin`} | `log` | linear or logarithmic scale|

_**`continuumRemoval`**_ : Method parameters to remove continuum of data spectra

| **Parameter name** | **Type** | **Default** | **Description** |
| - | - | - | - |
| `  .method` | {`zero`, `IrregularSamplingMedian`} | `IrregularSamplingMedian`| continuum estimation method. See also `linemodelsolve.linemodel.continuumcomponent` |
| `  .medianKernelWidth` |`float` |`400` | relevant only for median (in Angströms)|

_**`linemodelsolve.linemodel`**_ : Parameters for linemodel method

| **Parameter name** | **Type** | **Default** | **Description** |
| - | - | - | - |
| `  .linetypefilter` |{`no`,`E`,`A`} |`no`|  restrict the type of line to fit (`no`: fit all)|
| `  .lineforcefilter`|{`no`,`W`,`S`} |`no` | restrict the strength category of lines to fit (`no`: fit all)|
| `  .instrumentresolution` |`float` | `4300`| intrument resolution (R)|
| `  .velocityemission` |`float`| `200`| emission lines velocity (in $km \cdot s^{-1}$)|
| `  .velocityabsorption` |`float` |`300` | absorption lines velocity (in $km \cdot s^{-1}$)}|
| `  .velocityfit` |{`yes`,`no`} |`yes` | decide wether the 2nd pass include line width fitting|
| `  .emvelocityfitmin` |`float` |`10` | min velocity for emission line width $km \cdot s^{-1}$|
| `  .emvelocityfitmax` |`float` |`400`| max velocity for emission line width $km \cdot s^{-1}$|
| `  .emvelocityfitstep` |`float` |`20`| tabulation of velocity for emission line width fitting in $km \cdot s^{-1}$|
| `  .absvelocityfitmin` |`float` |`150`| min velocity for absorption line width $km \cdot s^{-1}$|
| `  .absvelocityfitmax` |`float` |`500`| max velocity for absorption line width $km \cdot s^{-1}$|
| `  .absvelocityfitstep` |`float` |`50`| tabulation of velocity for absorption line width fitting in $km \cdot s^{-1}$|
| `  .tplratio_ismfit` |{`yes`,`no`}|`yes` | activate fit of ISM extinction _i.e._ Ebv parameter from Calzetti profiles. Parameter scan from 0 to 0.9, step = 0.1. (best value stored in FittedTplshapeIsmCoeff in `linemodelsolve.linemodel_extrema.csv`)|
| `  .continuumcomponent` |{`fromspectrum`,`tplfit`} | `tplfit`  | select the method for processing the continuum:<br>&bull; `fromspectrum`: remove an estimated continuum (the continuum estmation is then tuned via `continuumRemoval` parameters). The redshift is thus only estimated from the lines.<br>&bull; `tplfit`: fit a set of redshifted template (aka `fullmodel` _i.e._ contiuum model + line model|
| `  .continuumfit.ismfit` |{`yes`,`no`} |`yes` | activate fit of ISM extinction _i.e._. Ebv parameter from Calzetti profiles. Parameter scan from 0 to 0.9, step = 0.1. (best value stored in FittedTplDustCoeff in `linemodelsolve.linemodel_extrema.csv`)  |
| `  .continuumfit.igmfit` |{`yes`,`no`} |`yes` | activate fit of IGM with Meiksin tables. Index scan from 0 to 0.9, step = 0.1 ??? (best profile index stored in FittedTplMeiksinIdx parameter in `linemodelsolve.linemodel_extrema.csv`)|
| `  .skipsecondpass` |{`yes`,`no`} |`no` | toggle the processing  of a second pass refined pass arround the candidates (`no` by default) |
| `  .extremacount` |`int` |`5` |Number of candidates to retain |
| `  .extremacutprobathreshold` |`float` |`30` |Select the number of candidates to refine at the 2nd pass.<br>&bull; `-1`: retain a fixed number (set from `extremacount` parameter)<br>&bull; any positive value: retain all candidates with $log(max(pdf))-log(pdf)$ values (not integrated)  below this threshold|
| `  .firstpass.tplratio_ismfit` |{`yes`,`no`} | `no` | overwrite the `tplratio_ismfit` parameter|
| `  .stronglinesprior` |`float` |`-1` | strongline prior<br>&bull; `-1`: no prior<br>&bull; otherwise, use this value (positive below 1) as a low probability when no strong line is measured (the measured amplitude is s>0) & probability is set to 1 when a strong line is observed|


## Outputs

### ```pfsZCandidates```

See [datamodel](https://github.com/Subaru-PFS/datamodel/blob/master/datamodel.txt "Subaru-PFS datamodel).

The header contains a quality flag, ZWARNING, with only one digit :

| **Bit name** | **Binary digit** | **Description** |
| - | - | - | - |
|LITTLE_COVERAGE|0|too little wavelength coverage (less than 3000 valid points)|
