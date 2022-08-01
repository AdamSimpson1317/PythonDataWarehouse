"""Parser for CSV titles/rows."""


def clean_row(row):
    """Return parse list of fields given a single CSV row.
    Removes all ' and \\\ n (new lines) from fields.
    """
    return row.strip().replace("'", "").replace("\n", "").split(",")


def clean_title(title):
    """Return a schema list when given a CSV title row.
    Replaces all spaces with _ and removes all ' and \\\ n (new lines). Finally makes all fields lowercase.
    """
    return title.strip().replace("'", "").replace("\n", "").replace(" ", "_").lower().split(",")
