from pylibamazed.redshift import WarningCode, ErrorCode
import json

def get_warning_codes():
    ret = dict()
    for wc in WarningCode:
        ret[wc.value]=wc.name
    return ret


def get_error_codes():
    ret = dict()
    for ec in ErrorCode:
        ret[ec.value]=ec.name
    return ret

def get_infos():
    return {"WarningCodes":get_warning_codes(),
            "ErrorCodes":get_error_codes()}


def main():
    """Pipeline entry point

    Return
    ------
    int
        Exit code of the main method
    """
    print(get_infos())
    
if __name__ == '__main__':
    main()
