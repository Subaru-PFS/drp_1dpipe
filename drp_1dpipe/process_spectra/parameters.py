default_parameters = {
    'lambdarange': [3000, 13000],
    'redshiftrange': [0.0, 6.0],
    'redshiftstep': 0.0001,
    'redshiftsampling': 'log',
    'method': 'linemodel',
    'templateCategoryList': ['emission', 'galaxy', 'star', 'qso'],
    'templateCatalog': {
        'continuumRemoval': {
            'method': 'zero',
            'medianKernelWidth': 75,
            'decompScales': 8,
            'binPath': ''
        }
    },
    'continuumRemoval': {
        'method': 'IrregularSamplingMedian',
        'medianKernelWidth': 400,
        'decompScales': 9,
        'binPath': ''
    },
    'linemodelsolve': {
        'linemodel': {
            'linetypefilter': 'no',
            'lineforcefilter': 'no',
            'fittingmethod': 'individual',
            'linewidthtype': 'combined',
            'instrumentresolution': 4300,
            'velocityemission': 200,
            'velocityabsorption': 300,
            'velocityfit': 'yes',
            'pdfcombination': 'marg',
            'emvelocityfitmin': 10,
            'emvelocityfitmax': 400,
            'emvelocityfitstep': 20,
            'absvelocityfitmin': 150,
            'absvelocityfitmax': 500,
            'absvelocityfitstep': 50,
            'lyaforcefit': 'no',
            'lyaforcedisablefit': 'yes',
            'lyafit': {
                'asymfitmin': 0,
                'asymfitmax': 4,
                'asymfitstep': 1,
                'widthfitmin': 1,
                'widthfitmax': 4,
                'widthfitstep': 1,
                'deltafitmin': 0,
                'deltafitmax': 0,
                'deltafitstep': 1
            },
            'rigidity': 'tplshape',
            'rules': 'all',
            'tplratio_catalog': ('linecatalogs_tplshapes/linecatalogs_tplshape'
                                 '_ExtendedTemplatesJan2017v3_20170602_B14C_'
                                 'v5_emission'),
            'tplratio_ismfit': 'no',
            'offsets_catalog': ('linecatalogs_offsets/'
                                'offsetsCatalogs_20170410_m150'),
            'continuumcomponent': 'tplfit',
            'continuumreestimation': 'no',
            'continuumfit': {
                'ismfit': 'yes',
                'igmfit': 'yes',
                'count': 1,
                'ignorelinesupport': 'no',
                'priors': {
                    'beta': 1,
                    'catalog_reldirpath': ''
                },
            },
            'skipsecondpass': 'no',
            'extremacount': 5,
            'extremacutprobathreshold': 30,
            'modelpriorzStrength': -1,
            'firstpass': {
                'fittingmethod': 'individual',
                'largegridstep': 0.001,
                'tplratio_ismfit': 'yes',
                'multiplecontinuumfit_disable': 'yes',
            },
            'secondpass': {
                'continuumfit': 'retryall',
            },
            'secondpasslcfittingmethod': -1,
            'manvelocityfitdzmin': -0.0006,
            'manvelocityfitdzmax': 0.0006,
            'manvelocityfitdzstep': 0.0001,
            'stronglinesprior': -1,
            'euclidnhaemittersStrength': -1,
            'pdfcombination': 'marg',
            'saveintermediateresults': 'no',
        }
    },
    'enablestellarsolve': 'no',
    'enableqsosolve': 'no',
    'calibrationDir': '',
    'SaveIntermediateResults': 'default',
    'linemeascatalog': '',
    'autocorrectinput': 'no'
}
