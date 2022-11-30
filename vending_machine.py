import sqlite3
from _sqlite3 import Error
from _sqlite3 import OperationalError
import pandas as pd
from pandas import DataFrame
import os
import sys


def create_connection(db_file):
    """Creates connection to database
        returns error if no connection is established"""

    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)

    return conn


def create_new_db(path):
    """Creates a new database at given location"""

    conn = sqlite3.connect(path)
    conn.commit()
    conn.close()


def do_math(conn, table):
    """Quick math function"""
    pd.set_option("display.max_rows", None)
    pd.set_option("display.max_columns", None)
    c = conn.cursor()
    c.execute("""SELECT Product, Retail_Price - Whole_Sale_Cost as Margin FROM '{0}'""".format(table))
    df = DataFrame(c.fetchall(), columns=['Product', 'Margin'])
    return print(df)


def show_all_data(conn, table):
    """Displays all data in a given table, columns wrap, difficult to read"""

    pd.set_option("display.max_rows", None)
    pd.set_option("display.max_columns", None)
    c = conn.cursor()
    c.execute("""SELECT * FROM '{0}'""".format(table))
    df = DataFrame(c.fetchall(), columns=['Product', 'Category', 'Whole_Sale_Cost', 'Retail_Price', 'Quantity'])
    return print(df)


def print_data(conn, table):
    """Displays all rows in the given table, only shows Product, Quantity, and EXP"""

    pd.set_option('display.max_rows', None)
    c = conn.cursor()
    c.execute("""SELECT Product, QUANTITY FROM '{0}'""".format(table))
    df = DataFrame(c.fetchall(), columns=["Product", "Quantity"])
    return print(df)


def show_tables(conn):
    """Displays all tables in the database"""

    c = conn.cursor()
    c.execute("""SELECT name FROM sqlite_master
                WHERE type='table'
                ORDER BY name""")
    df = DataFrame(c.fetchall(), columns=["name"])
    return print(df)


def check_tables(conn, table):
    """Checks to see if a table is in the database, if not returns error"""

    c = conn.cursor()
    c.execute("""SELECT name FROM "main".sqlite_master where name = '{0}'""".format(table))
    if len(c.fetchall()) > 0:
        return True
    else:
        raise ValueError


def make_table(conn, table_name):

    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS {0} (
                "Product" TEXT NOT NULL,
                "Category" TEXT NOT NULL, 
                "Whole_Sale_Cost" DECIMAL(5,2) NOT NULL,
                "Retail_Price" DECIMAL(5,2) NOT NULL, 
                "Quantity" INTEGER)
                """.format(table_name))
    conn.commit()
    print_data(conn, table_name)


def drop_table(conn, table):
    """Drops a selected table"""

    c = conn.cursor()
    c.execute("""DROP TABLE {0}""".format(table))
    conn.commit()
    show_tables(conn)


def csv_insert(conn, file, table):
    """Fills a table from a .csv file"""

    c = conn.cursor()

    read_clients = pd.read_csv(r"{}".format(file))  # CSV FIle
    read_clients.to_sql('{}'.format(table), conn, if_exists='append', index=False)

    c.execute("""INSERT INTO {0} (PRODUCT, CATEGORY, WHOLE_SALE_COST, RETAIL_PRICE, QUANTITY)
            SELECT PRODUCT, CATEGORY, WHOLE_SALE_COST, RETAIL_PRICE, QUANTITY
            FROM {1}""".format(table, table))

    conn.commit()
    print_data(conn, table)


def add_item(conn, table):
    """Adds a single item into a given table"""

    c = conn.cursor()
    product = input('Product? ')
    category = input('Category? ')
    whole_sale = input('Whole sale cost? ')
    retail_price = input('Retail Price? ')
    quantity = input('Quantity ')
    c.execute("""Insert into '{0}' ( PRODUCT, CATEGORY, WHOLE_SALE_COST, RETAIL_PRICE, QUANTITY)
            VALUES ('{1}', '{2}', '{3}', '{4}', '{5}')""".format(table, product, category,
                                                                whole_sale, retail_price, quantity))

    conn.commit()
    print_data(conn, table)


def remove_item(conn, table, item):
    """Removes a single item from a table, using the Product column"""

    c = conn.cursor()
    c.execute("""DELETE FROM '{0}' WHERE PRODUCT = '{1}'""".format(table, item))

    conn.commit()
    print_data(conn, table)


def adjust_quantity(conn, table, item, quantity):
    """Updates the Quantity of a given product in a given table"""

    c = conn.cursor()
    c.execute("""UPDATE {0} SET Quantity = '{1}' WHERE Product = '{2}' """.format(table, quantity, item))
    conn.commit()
    print_data(conn, table)


def find_db():
    """First scans directory where the program resides, if none is found asks for another location. If no
        other database is present one is able to be created"""

    db = None
    while not db:
        try:
            with os.scandir(os.getcwd()) as it:
                for entry in it:
                    if entry.name.endswith('.db'):
                        db = os.path.join(os.getcwd(), entry.name)
                        return db
                else:
                    raise FileExistsError
        except FileExistsError:
            print("No database found here")
            new_db = input("Is there another location for \n"
                           "existing database? (Yes, No)\n"
                           "or exit\n").lower()
            if "no" in new_db:
                create_db = input("Database path and name?\n"
                                  "ex: /Users/user/Desktop/test.db\n")
                create_new_db(create_db)
                db = create_connection(create_db)
                return db
            elif "yes" in new_db:
                existing_db = input("Path of existing Database?\n")
                if existing_db.endswith(".db") and os.path.exists(existing_db):
                    db = existing_db
                    return db
            elif "exit" in new_db:
                sys.exit()
            break


def __main__():

    database = None
    while not database:
        database = find_db()
        if database:
            break
    session = True
    conn = create_connection(database)

    while session:
        try:
            print("You are working in {} Database!".format(database.rsplit('/', 1)[1]))
            process = input("What would you like to do?\n"
                            "* Note, phrases must be typed exactly *\n\n"
                            "Make a new table?\n"
                            "Insert file into table?\n"
                            "Insert single item?\n"
                            "Adjust Quantity?\n"
                            "Remove item?\n"
                            "Show all tables?\n"
                            "Print table contents?\n"
                            "Remove table?\n"
                            "Do Math? \n"
                            "or exit?\n").lower()

            if process == "make a new table":
                table_loop = True
                while table_loop:
                    try:
                        print("Current Tables:")
                        show_tables(conn)
                        new_table = input("What is the name of the new table?\n")
                        make_table(conn, new_table)
                        table_loop = input("Finished adding tables?\n").lower()
                        if "yes" in table_loop:
                            break

                    except OperationalError:
                        print("Something went wrong, lets try that again!")

            if process == "remove table":
                remove_table_loop = True
                while remove_table_loop:
                    try:
                        show_tables(conn)
                        rmv_table = input("What table are we removing?\n")
                        if check_tables(conn, rmv_table):
                            drop_table(conn, rmv_table)
                            remove_table_loop = input("Finished removing tables?\n").lower()
                            if "yes" in remove_table_loop:
                                break

                    except OperationalError:
                        print("Something went wrong, lets try that again!")

            if process == "insert file into table":
                insert_loop = True
                while insert_loop:
                    try:
                        file_loc = input("What is the path of the .csv file?\n")
                        if file_loc.endswith(".csv") and os.path.exists(file_loc):
                            show_tables(conn)
                            tbl = input("Which table do you want to import file to?\n")
                            if check_tables(conn, tbl):
                                csv_insert(conn, file_loc, tbl)
                                insert_loop = input("Finished importing files?\n").lower()
                                if "yes" in insert_loop:
                                    break
                        else:
                            raise FileNotFoundError

                    except FileNotFoundError:
                        print("File not found or not .csv\n"
                              "Please try again!\n")
                    except OperationalError:
                        print("Table not found in Database, please try again!")

            if process == "insert single item":
                single_add_loop = True
                while single_add_loop:
                    try:
                        show_tables(conn)
                        insert_item_table = input("What Table would you like to add to?\n")
                        if check_tables(conn, insert_item_table):
                            add_item(conn, insert_item_table)
                            single_add_loop = input("Finished add items?\n").lower()
                            if "yes" in single_add_loop:
                                break

                    except OperationalError:
                        print("Table not found in Database, or value incorrect type.\n"
                              "Please try again!")
                    except ValueError:
                        print("Table not in database")

            if process == "adjust quantity":
                adjust_loop = True
                while adjust_loop:
                    try:
                        show_tables(conn)
                        adj_table = input("What table is the product in?\n")
                        print_data(conn, adj_table)
                        adj_item = input("What item are we adjusting?\n")
                        adj_quantity = input("What is the new quantity?\n")
                        if check_tables(conn, adj_table):
                            adjust_quantity(conn, adj_table, adj_item, adj_quantity)
                            adjust_loop = input("Finished adjusting items?\n").lower()
                            if 'yes' in adjust_loop:
                                adjust_loop = False
                                break

                    except OperationalError:
                        print("Something went wrong, please try again!")

            if process == "remove item":
                rmv_itm_loop = True
                while rmv_itm_loop:
                    try:
                        show_tables(conn)
                        rmv_itm_table = input("What table is the item in?\n")
                        print_data(conn, rmv_itm_table)
                        item = input("What item are we removing?\n"
                                     "* (If item is not in list no error will show) *\n")
                        if check_tables(conn, rmv_itm_table):
                            remove_item(conn, rmv_itm_table, item)
                            rmv_itm_loop = input("Finished deleting items?\n")
                            if "yes" in rmv_itm_loop:
                                break
                    except OperationalError:
                        print("Table not found in Database, or value incorrect type.\n"
                              "Please try again!")
                    except ValueError:
                        print("Item not in table")

            if process == "show all tables":
                show_tables(conn)

            if process == "do math":
                show_tables(conn)
                math_table = input("What table do you want to do mathn on?\n")
                do_math(conn, math_table)

            if process == "print table contents":
                show_tables(conn)
                print_table = input("What table do you want to see?\n")
                show_all_data(conn, print_table)

            if process == "exit":
                conn.close()
                sys.exit()

            session = input("Are you finished with Database {}?\n".format(database.rsplit('/', 1)[1]))
            if 'yes' in session:
                break

        except (FileNotFoundError, FileExistsError):
            print("Something went wrong, please try again!")


if __name__ == "__main__":
    __main__()
