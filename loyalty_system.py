import sqlite3
from datetime import datetime

# Функція для створення підключення до бази даних
def create_connection():
    # Підключаємось до бази даних (створює файл loyalty_system.db, якщо його не існує)
    connection = sqlite3.connect('loyalty_system.db')
    return connection

# Функція для створення таблиць у базі даних
def create_tables(connection):
    cursor = connection.cursor()

    # Створення таблиці клієнтів
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Customers (
        CustomerID INTEGER PRIMARY KEY AUTOINCREMENT,
        Name TEXT,
        Email TEXT,
        Phone TEXT
    )
    """)

    # Створення таблиці бонусних карток, пов'язаної з таблицею Customers
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS LoyaltyCards (
        CardID INTEGER PRIMARY KEY AUTOINCREMENT,
        CustomerID INTEGER,
        BonusPoints INTEGER DEFAULT 0,
        IssueDate DATE,
        FOREIGN KEY (CustomerID) REFERENCES Customers(CustomerID)
    )
    """)

    # Створення таблиці товарів
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Products (
        ProductID INTEGER PRIMARY KEY AUTOINCREMENT,
        ProductName TEXT,
        Price REAL
    )
    """)

    # Створення таблиці замовлень, пов'язаної з таблицею Customers
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Orders (
        OrderID INTEGER PRIMARY KEY AUTOINCREMENT,
        CustomerID INTEGER,
        OrderDate DATE,
        TotalAmount REAL,
        BonusUsed INTEGER,
        FOREIGN KEY (CustomerID) REFERENCES Customers(CustomerID)
    )
    """)

    # Створення таблиці транзакцій, пов'язаної з таблицями LoyaltyCards та Orders
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Transactions (
        TransactionID INTEGER PRIMARY KEY AUTOINCREMENT,
        CardID INTEGER,
        OrderID INTEGER,
        TransactionDate DATE,
        PointsEarned INTEGER,
        PointsSpent INTEGER,
        FOREIGN KEY (CardID) REFERENCES LoyaltyCards(CardID),
        FOREIGN KEY (OrderID) REFERENCES Orders(OrderID)
    )
    """)
    connection.commit()  # Зберігаємо зміни в базі даних

# Функція для заповнення бази даних початковими даними
def insert_initial_data(connection):
    cursor = connection.cursor()

    # Додавання клієнтів до таблиці Customers
    cursor.executemany("INSERT INTO Customers (Name, Email, Phone) VALUES (?, ?, ?)", [
        ('John Doe', 'john@example.com', '+123456789'),
        ('Jane Smith', 'jane@example.com', '+987654321')
    ])

    # Додавання бонусних карток до таблиці LoyaltyCards
    cursor.executemany("INSERT INTO LoyaltyCards (CustomerID, BonusPoints, IssueDate) VALUES (?, ?, ?)", [
        (1, 100, '2024-01-15'),
        (2, 50, '2024-02-10')
    ])

    # Додавання товарів до таблиці Products
    cursor.executemany("INSERT INTO Products (ProductName, Price) VALUES (?, ?)", [
        ('Milk', 1.99),
        ('Bread', 0.99),
        ('Eggs', 2.49)
    ])

    # Додавання замовлень до таблиці Orders
    cursor.executemany("INSERT INTO Orders (CustomerID, OrderDate, TotalAmount, BonusUsed) VALUES (?, ?, ?, ?)", [
        (1, '2024-03-05', 15.00, 10),
        (2, '2024-03-06', 20.00, 5)
    ])

    # Додавання транзакцій до таблиці Transactions
    cursor.executemany(
        "INSERT INTO Transactions (CardID, OrderID, TransactionDate, PointsEarned, PointsSpent) VALUES (?, ?, ?, ?, ?)",
        [
            (1, 1, '2024-03-05', 20, 10),
            (2, 2, '2024-03-06', 30, 5)
        ])
    connection.commit()  # Зберігаємо зміни в базі даних

# Функція для відображення всіх даних з вказаної таблиці
def view_table(connection, table_name):
    cursor = connection.cursor()
    cursor.execute(f"SELECT * FROM {table_name}")
    rows = cursor.fetchall()
    print(f"Дані з таблиці {table_name}:")
    for row in rows:
        print(row)
    print()

# JOIN-запит для об'єднання даних з таблиць Orders, Customers і Transactions
def get_orders_with_transactions(connection):
    cursor = connection.cursor()
    query = """
    SELECT Orders.OrderID, Customers.Name, Orders.TotalAmount, Transactions.PointsEarned
    FROM Orders
    JOIN Customers ON Orders.CustomerID = Customers.CustomerID
    JOIN Transactions ON Orders.OrderID = Transactions.OrderID
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    print("Замовлення та транзакції:")
    for row in rows:
        print(row)
    print()

# Запит з фільтрацією для відображення замовлень із сумою більше заданого значення
def get_high_value_orders(connection, min_amount):
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM Orders WHERE TotalAmount > ?", (min_amount,))
    rows = cursor.fetchall()
    print(f"Замовлення з сумою більше {min_amount}:")
    for row in rows:
        print(row)
    print()

# Агрегатна функція для підрахунку загальної кількості бонусних балів
def get_total_bonus_points(connection):
    cursor = connection.cursor()
    cursor.execute("SELECT SUM(BonusPoints) FROM LoyaltyCards")
    result = cursor.fetchone()
    print(f"Загальна кількість бонусних балів: {result[0]}\n")

# Головна функція для запуску програми
def main():
    # Створення підключення до бази даних
    connection = create_connection()

    # Створення таблиць та заповнення даними
    create_tables(connection)
    insert_initial_data(connection)

    # Відображення даних з таблиць
    view_table(connection, "Customers")
    view_table(connection, "LoyaltyCards")
    view_table(connection, "Orders")

    # Виконання запитів
    get_orders_with_transactions(connection)
    get_high_value_orders(connection, 10)
    get_total_bonus_points(connection)

    # Закриття підключення до бази даних
    connection.close()

# Виконання головної функції при запуску скрипта
if __name__ == "__main__":
    main()
