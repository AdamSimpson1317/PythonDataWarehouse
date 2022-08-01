import ParseCSV

DATASET_FILE = "resources/Date_Data.csv"
TABLE_NAME = "DateDim"


def create_table(cursor):
    file = open(DATASET_FILE, "r")

    schema = ParseCSV.clean_title(file.readline())

    create_table_statement = f"""
        CREATE TABLE {TABLE_NAME} (
            {schema[0]} INTEGER PRIMARY KEY,
            {schema[1]} TEXT,
            {schema[2]} TEXT,
            {schema[3]} INTEGER,
            {schema[4]} TEXT,
            {schema[5]} TEXT,
            {schema[6]} INTEGER,
            {schema[7]} INTEGER,
            {schema[8]} INTEGER,
            {schema[9]} INTEGER,
            {schema[10]} TEXT,
            {schema[11]} TEXT,
            {schema[12]} INTEGER
        )
    """

    cursor.execute(create_table_statement)

    line = file.readline()
    while line is not None and line != "":
        insert_statement = f"INSERT INTO {TABLE_NAME} (" + ','.join(schema) + ") VALUES (" + ",".join(
            ["?" for _ in range(len(schema))]) + ")"

        fields = ParseCSV.clean_row(line)
        cursor.execute(insert_statement, fields)

        line = file.readline()
