# DRP_1DPIPE - User's Manual

## Installation

See README.md

## Configuration

### Runtime environment

Pipeline is run with the `drp_1dpipe` command.

#### Synopsis
```
drp_1dpipe [-h] [--workdir WORKDIR] [--logdir LOGDIR] [--spectra_path DIR]
           [--pre_commands COMMAND] [--loglevel LOGLEVEL]
		   [--scheduler SCHEDULER] [--bunch_size SIZE]
```
#### Optional arguments

Arguments can be given on the command line and in the configuration file `drp_1dpipe/io/conf/drp_1dpipe.conf`. Command line arguments take precedence.

##### `--workdir WORKDIR`

The root working directory where data is located.

##### `--logdir LOGDIR`

The logging directory.

##### `--loglevel LOGLEVEL`

The logging level. `CRITICAL`, `ERROR`, `WARNING`, `INFO` or `DEBUG`.

##### `--scheduler SCHEDULER`

The scheduler to use. Either `local` or `pbs`.

##### `--pre_commands COMMAND`

Commands to run before before `process_spectra`. This gives the opportunity for each process to initialize a virtualenv or mount a data directory.
`COMMAND` is a list of commands given as a python expression.

##### `--spectra_path DIR`

Base path where to find spectra. Relative to workdir.

##### `--bunch_size SIZE`

Maximum number of spectra per bunch.

##### `-h`, `--help`

Show the help message and exit.

#### Examples

#####

Run using all defaults from `scheduler.conf`:

```sh
drp_1dpipe
```

#####

Run on a PBS queue, activating a virtualenv on each node before running:

```sh
drp_1dpipe --scheduler pbs \
           --pre_commands "source $HOME/venv/bin/activate"
```

### Algorithmic parameters

Redshift determination algorithms can be tuned using an optional JSON file.
Defaults to `drp_1dpipe/io/auxdir/parameters.json`. An example file can be found in `drp_1dpipe/io/auxdir/parameters.json.example`.

Parameters are as follow :

| **Parameter name** | **Type** | **Default** | **Description** |
| --- | --- | --- | --- |
| _**general**_  ||| _**parameters always applicable**_
| `lambdarange` | `["min", "max"]` | `[ "3000", "13000"]` | lambda range in Angströms
| `redshiftrange` | `["min", "max"]` | `[ "0.0", "6."]` | resdshift range
| `redshiftstep` | `float` | `0.0001` | redshift step for linear scale or lowest step for log scale
| `redshiftsampling` |`log` / `lin` | `log` | linear or logarithmic scale
|`calibrationDir` | | | |
||
| _**`continuumRemoval`**_ ||| _**Method parameters to remove continuum of data spectra**_
| `continuumRemoval.method` | `zero` / `median` / `irregularSamplingMedian` / `raw` | `zero`| continuum estimation method|
| `continuumRemoval.medianKernelWidth` |`float` |`400` | relevant only for median (in Angströms)|
||
|  _**`linemodelsolve.linemodel`**_ ||| _**parameters for linemodel**_
| `linemodelsolve.linemodel.linetypefilter` |`no` / `E` / `A` |`no`|  restrict the type of line to fit (`no`: fit all)
| `linemodelsolve.linemodel.lineforcefilter`|`no` / `W` / `S` |`no` | restrict the strength category of lines to fit (`no`: fit all)
| `linemodelsolve.linemodel.instrumentresolution` |`float` | `4300`| intrument resolution (R)
| `linemodelsolve.linemodel.velocityemission` |`float`| `200`| emission lines velocity (in $km \cdot s^{-1}$)
| `linemodelsolve.linemodel.velocityabsorption` |`float` |`300` | absorption lines velocity (in $km \cdot s^{-1}$)
| `linemodelsolve.linemodel.velocityfit` |`yes` / `no` |`yes` | decide wether the 2nd pass include line width fitting
| `linemodelsolve.linemodel.emvelocityfitmin` |`float` |`10` | tabulation of velocity for line width fitting in $km \cdot s^{-1}$
| `linemodelsolve.linemodel.emvelocityfitmax` |`float` |`400`
| `linemodelsolve.linemodel.emvelocityfitstep` |`float` |`20`
| `linemodelsolve.linemodel.absvelocityfitmin` |`float` |`150`
| `linemodelsolve.linemodel.absvelocityfitmax` |`float` |`500`
| `linemodelsolve.linemodel.absvelocityfitstep` |`float` |`50`
| `linemodelsolve.linemodel.tplratio_ismfit` |`yes`/`no` |`no` | activate fit of ISM extinction _i.e._ Ebv parameter from Calzetti profiles. Parameter scan from 0 to 0.9, step = 0.1. (best value stored in FittedTplshapeIsmCoeff in `linemodelsolve.linemodel_extrema.csv`)
| `linemodelsolve.linemodel.continuumcomponent` |`fromspectrum` / `tplfit` | `tplfit`  | select the method for processing the continuum:<br>&bull; `fromspectrum`: remove an estimated continuum (the continuum estmation is then tuned via `continuumRemoval` parameters). The redshift is thus only estimated from the lines.<br>&bull; `tplfit`: fit a set of redshifted template (aka `fullmodel` _i.e._ contiuum model + line model
| `linemodelsolve.linemodel.continuumfit.ismfit` |`yes`/`no` |`yes` | activate fit of ISM extinction _i.e._. Ebv parameter from Calzetti profiles. Parameter scan from 0 to 0.9, step = 0.1. (best value stored in FittedTplDustCoeff in `linemodelsolve.linemodel_extrema.csv`)  |
| `linemodelsolve.linemodel.continuumfit.igmfit` |`yes`/`no` |`yes` | activate fit of IGM with Meiksin tables. Index scan from 0 to 0.9, step = 0.1 ??? (best profile index stored in FittedTplMeiksinIdx parameter in `linemodelsolve.linemodel_extrema.csv`)
| `linemodelsolve.linemodel.skipsecondpass` |`yes` / `no` |`no` | toggle the processing  of a second pass refined pass arround the candidates (`no` by default) |
| `linemodelsolve.linemodel.extremacount` |`int` |`5` |Number of candidates to retain |
| `linemodelsolve.linemodel.extremacutprobathreshold` |`float` |`30` |Select the number of candidates to refine at the 2nd pass.<br>&bull; `-1`: retain a fixed number (set from `extremacount` parameter)<br>&bull; any positive value: retain all candidates with `log(max(pdf))-log(pdf)` values (not integrated)  below this threshold
| `linemodelsolve.linemodel.firstpass.tplratio_ismfit` |`yes` / `no` | `no` | overwrite the `tplratio_ismfit` parameter
||
| `linemodelsolve.linemodel.stronglinesprior` |`float` |`-1` | strongline prior<br>&bull; `-1`: no prior<br>&bull; otherwise, use this value (positive below 1) as a low probability when no strong line is measured (the measured amplitude is s>0) & probability is set to 1 when a strong line is observed


## Outputs

### ```pfsZCandidates```

See [datamodel](https://github.com/Subaru-PFS/datamodel/blob/master/datamodel.txt "Subaru-PFS datamodel).
