import os

def get_default_summary_columns():
    summary_path = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                                "../auxdir/summary.conf")

    with open(summary_path) as f:
        summary_columns = f.read().split("\n")
    return summary_columns
