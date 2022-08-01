import ParseCSV

DATASET_FILE = "resources/EduBase_Schools_July_2017_Data.csv"
TABLE_NAME = "SchoolDim"


def create_table(cursor):
    file = open(DATASET_FILE, "r")

    schema = ParseCSV.clean_title(file.readline())

    create_table_statement = f"""
        CREATE TABLE {TABLE_NAME} (
            {schema[0]} INTEGER PRIMARY KEY,
            {schema[1]} INTEGER,
            {schema[2]} TEXT,
            {schema[3]} INTEGER,
            {schema[4]} TEXT,
            {schema[5]} TEXT,
            {schema[6]} TEXT,
            {schema[7]} TEXT,
            {schema[8]} TEXT,
            {schema[9]} TEXT,
            {schema[10]} INTEGER,
            {schema[11]} INTEGER,
            {schema[12]} TEXT,
            {schema[13]} TEXT,
            {schema[14]} TEXT,
            {schema[15]} TEXT,
            {schema[16]} TEXT,
            {schema[17]} TEXT,
            {schema[18]} TEXT,
            {schema[19]} TEXT,
            {schema[20]} TEXT,
            {schema[21]} TEXT,
            {schema[22]} TEXT,
            {schema[23]} TEXT,
            {schema[24]} TEXT,
            {schema[25]} TEXT,
            {schema[26]} TEXT,
            {schema[27]} TEXT,
            {schema[28]} TEXT,
            {schema[29]} TEXT
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
