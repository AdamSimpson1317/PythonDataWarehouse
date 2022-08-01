import sqlite3

import Date_Dimension
import Fact_Table
import School_Dimension
import Session_Dimension
import Staff_Dimension


def main():
    db_name = "DataWarehouse"

    connection = sqlite3.connect(db_name + ".db")
    cursor = connection.cursor()

    Date_Dimension.create_table(cursor)
    School_Dimension.create_table(cursor)
    Session_Dimension.create_table(cursor)
    Staff_Dimension.create_table(cursor)

    # Create dimension tables
    connection.commit()

    # Fact_Table.create_fact_table(cursor)

    connection.commit()
    connection.close()


if __name__ == "__main__":
    main()
