import mysql.connector

def connect_db():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="Athiraanoop" 
    )

def create_database(cursor):
    cursor.execute("CREATE DATABASE IF NOT EXISTS e_commerce")
    cursor.execute("USE e_commerce")

def main():
    try:
        connection = connect_db()
        cursor = connection.cursor()
        print("Connected to MySQL")

        create_database(cursor)
        print("e_commerce database created or selected")

        create_tables(cursor)  

        insert_sample_data(cursor)

        run_queries(cursor)
    
        connection.commit() 
        
        connection.close()
    except mysql.connector.Error as err:
        print(f"Error: {err}")

def create_tables(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS supplier (
            SUPP_ID INT PRIMARY KEY,
            SUPP_NAME VARCHAR(50),
            SUPP_CITY VARCHAR(50),
            SUPP_PHONE VARCHAR(10)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS customer (
            CUS_ID INT PRIMARY KEY,
            CUS_NAME VARCHAR(20),
            CUS_PHONE VARCHAR(10),
            CUS_CITY VARCHAR(30),
            CUS_GENDER CHAR(1)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS category (
            CAT_ID INT PRIMARY KEY,
            CAT_NAME VARCHAR(20)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS product (
            PRO_ID INT PRIMARY KEY,
            PRO_NAME VARCHAR(20),
            PRO_DESC VARCHAR(60),
            CAT_ID INT,
            FOREIGN KEY (CAT_ID) REFERENCES category(CAT_ID)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS product_details (
            PROD_ID INT PRIMARY KEY,
            PRO_ID INT,
            SUPP_ID INT,
            PROD_PRICE INT,
            FOREIGN KEY (PRO_ID) REFERENCES product(PRO_ID),
            FOREIGN KEY (SUPP_ID) REFERENCES supplier(SUPP_ID)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            ORD_ID INT PRIMARY KEY,
            ORD_AMOUNT INT,
            ORD_DATE DATE,
            CUS_ID INT,
            PROD_ID INT,
            FOREIGN KEY (CUS_ID) REFERENCES customer(CUS_ID),
            FOREIGN KEY (PROD_ID) REFERENCES product_details(PROD_ID)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS rating (
            RAT_ID INT PRIMARY KEY,
            CUS_ID INT,
            SUPP_ID INT,
            RAT_RATSTARS INT,
            FOREIGN KEY (CUS_ID) REFERENCES customer(CUS_ID),
            FOREIGN KEY (SUPP_ID) REFERENCES supplier(SUPP_ID)
        )
    """)

    print("All tables created successfully.")


def insert_sample_data(cursor):
    cursor.execute("DELETE FROM rating")
    cursor.execute("DELETE FROM orders")
    cursor.execute("DELETE FROM product_details")
    cursor.execute("DELETE FROM product")
    cursor.execute("DELETE FROM category")
    cursor.execute("DELETE FROM customer")
    cursor.execute("DELETE FROM supplier")

    cursor.executemany("""
        INSERT INTO supplier (SUPP_ID, SUPP_NAME, SUPP_CITY, SUPP_PHONE)
        VALUES (%s, %s, %s, %s)
    """, [
        (1, 'Rajesh Retails', 'Delhi', '1234567890'),
        (2, 'Appario Ltd.', 'Mumbai', '2589631470'),
        (3, 'Knome products', 'Bangalore', '9785462315'),
        (4, 'Bansal Retails', 'Kochi', '8975463285'),
        (5, 'Mittal Ltd.', 'Lucknow', '7898456532')
    ])

    cursor.executemany("""
        INSERT INTO customer (CUS_ID, CUS_NAME, CUS_PHONE, CUS_CITY, CUS_GENDER)
        VALUES (%s, %s, %s, %s, %s)
    """, [
        (1, 'AAKASH', '9999999999', 'DELHI', 'M'),
        (2, 'AMAN', '9785463215', 'NOIDA', 'M'),
        (3, 'NEHA', '9999999998', 'MUMBAI', 'F'),
        (4, 'MEGHA', '9994562399', 'KOLKATA', 'F'),
        (5, 'PULKIT', '7895999999', 'LUCKNOW', 'M')
    ])

    cursor.executemany("""
        INSERT INTO category (CAT_ID, CAT_NAME)
        VALUES (%s, %s)
    """, [
        (1, 'BOOKS'), (2, 'GAMES'), (3, 'GROCERIES'),
        (4, 'ELECTRONICS'), (5, 'CLOTHES')
    ])

    cursor.executemany("""
        INSERT INTO product (PRO_ID, PRO_NAME, PRO_DESC, CAT_ID)
        VALUES (%s, %s, %s, %s)
    """, [
        (1, 'GTA V', 'DFJDJFDJFDJFDJFJF', 2),
        (2, 'TSHIRT', 'DFDFJDFJDKFD', 5),
        (3, 'ROG LAPTOP', 'DFNTTNTNTERND', 4),
        (4, 'OATS', 'REURENTBTOTH', 3),
        (5, 'HARRY POTTER', 'NBEMCTHTJTH', 1)
    ])

    cursor.executemany("""
        INSERT INTO product_details (PROD_ID, PRO_ID, SUPP_ID, PROD_PRICE)
        VALUES (%s, %s, %s, %s)
    """, [
        (1, 1, 2, 1500),
        (2, 3, 5, 30000),
        (3, 5, 1, 3000),
        (4, 2, 3, 2500),
        (5, 4, 1, 1000)
    ])

    cursor.executemany("""
        INSERT INTO orders (ORD_ID, ORD_AMOUNT, ORD_DATE, CUS_ID, PROD_ID)
        VALUES (%s, %s, %s, %s, %s)
    """, [
        (20, 1500, '2021-10-12', 3, 3),
        (25, 30500, '2021-09-16', 5, 4),
        (26, 2000, '2021-10-05', 1, 1),
        (30, 3500, '2021-08-16', 4, 2),
        (50, 2000, '2021-10-06', 2, 1)
    ])

    cursor.executemany("""
        INSERT INTO rating (RAT_ID, CUS_ID, SUPP_ID, RAT_RATSTARS)
        VALUES (%s, %s, %s, %s)
    """, [
        (1, 2, 2, 4),
        (2, 3, 4, 3),
        (3, 5, 1, 5),
        (4, 1, 3, 2),
        (5, 4, 5, 4)
    ])

    print("Sample data inserted.")

def run_queries(cursor):
    print("\nQ3: Number of customers grouped by gender with order amount ≥ 3000")
    cursor.execute("""
        SELECT CUS_GENDER, COUNT(DISTINCT customer.CUS_ID) AS num_customers
        FROM customer
        JOIN orders ON customer.CUS_ID = orders.CUS_ID
        WHERE ORD_AMOUNT >= 3000
        GROUP BY CUS_GENDER
    """)
    for row in cursor.fetchall():
        print(row)

    print("\nQ4: Orders with product name for customer ID = 2")
    cursor.execute("""
        SELECT orders.*, product.PRO_NAME
        FROM orders
        JOIN product_details ON orders.PROD_ID = product_details.PROD_ID
        JOIN product ON product_details.PRO_ID = product.PRO_ID
        WHERE orders.CUS_ID = 2
    """)
    for row in cursor.fetchall():
        print(row)

    print("\nQ5: Supplier details who supply more than one product")
    cursor.execute("""
        SELECT supplier.*
        FROM supplier
        JOIN product_details ON supplier.SUPP_ID = product_details.SUPP_ID
        GROUP BY supplier.SUPP_ID
        HAVING COUNT(DISTINCT product_details.PROD_ID) > 1
    """)
    for row in cursor.fetchall():
        print(row)

    print("\nQ6: Category of the product with minimum order amount")
    cursor.execute("""
        SELECT category.CAT_NAME
        FROM orders
        JOIN product_details ON orders.PROD_ID = product_details.PROD_ID
        JOIN product ON product_details.PRO_ID = product.PRO_ID
        JOIN category ON product.CAT_ID = category.CAT_ID
        ORDER BY orders.ORD_AMOUNT ASC
        LIMIT 1
    """)
    for row in cursor.fetchall():
        print(row)

    print("\nQ7: ID and Name of products ordered after 2021-10-05")
    cursor.execute("""
        SELECT DISTINCT product.PRO_ID, product.PRO_NAME
        FROM orders
        JOIN product_details ON orders.PROD_ID = product_details.PROD_ID
        JOIN product ON product_details.PRO_ID = product.PRO_ID
        WHERE ORD_DATE > '2021-10-05'
    """)
    for row in cursor.fetchall():
        print(row)

    print("\nQ8: Top 3 suppliers by rating with customer names")
    cursor.execute("""
        SELECT supplier.SUPP_ID, supplier.SUPP_NAME, rating.RAT_RATSTARS, customer.CUS_NAME
        FROM rating
        JOIN supplier ON rating.SUPP_ID = supplier.SUPP_ID
        JOIN customer ON rating.CUS_ID = customer.CUS_ID
        ORDER BY RAT_RATSTARS DESC
        LIMIT 3
    """)
    for row in cursor.fetchall():
        print(row)

    print("\nQ9: Customers whose names start or end with 'A'")
    cursor.execute("""
        SELECT CUS_NAME, CUS_GENDER
        FROM customer
        WHERE CUS_NAME LIKE 'A%' OR CUS_NAME LIKE '%A'
    """)
    for row in cursor.fetchall():
        print(row)

    print("\nQ10: Total order amount of male customers")
    cursor.execute("""
        SELECT SUM(ORD_AMOUNT) 
        FROM orders
        JOIN customer ON orders.CUS_ID = customer.CUS_ID
        WHERE customer.CUS_GENDER = 'M'
    """)
    for row in cursor.fetchall():
        print(row)

    print("\nQ11: All customers LEFT OUTER JOIN orders")
    cursor.execute("""
        SELECT customer.*, orders.ORD_ID, orders.ORD_AMOUNT, orders.ORD_DATE
        FROM customer
        LEFT JOIN orders ON customer.CUS_ID = orders.CUS_ID
    """)
    for row in cursor.fetchall():
        print(row)




if __name__ == "__main__":
    main()
