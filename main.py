import mysql.connector
import uuid
from faker import Faker
from dotenv import load_dotenv
import random
from datetime import datetime, timedelta
import os

# Load environment variables
load_dotenv()

# Connection settings
HOST = os.getenv('host')
USER = os.getenv('user')
PASSWORD = os.getenv('password')
DATABASE = os.getenv('database')


# Connect to the MySQL database
connection = mysql.connector.connect(
    host='localhost',
    user='root',
    password='qwerty',
    database='pa02'
)

cursor = connection.cursor()
fake = Faker()

# Insert 1,000,000 rows into opt_clients
print("Inserting into users table...")
client_insert_query = """
    INSERT INTO users (username, country, email, phone) 
    VALUES (%s, %s, %s, %s)
"""
clients_data = [
    (fake.first_name(), fake.country(), fake.email(), fake.phone_number())
    for _ in range(100000)
]
cursor.executemany(client_insert_query, clients_data)
connection.commit()
print("Inserted into users.")

# Insert 1,000 rows into opt_products
print("Inserting into subscriptions...")

# Fetch unique user_ids from the users table
cursor.execute("SELECT user_id FROM users")
user_ids = [user_id[0] for user_id in cursor.fetchall()]  # Assuming the user_id is the first column returned

# Shuffle user_ids to randomize the subscription assignment
random.shuffle(user_ids)

# Prepare the insert query
product_insert_query = """
    INSERT INTO subscriptions (user_id, type_of_plan, subscription_start, subscription_end) 
    VALUES (%s, %s, %s, %s)  # Make sure to include all necessary fields
"""

types_of_plans = ['basic', 'free', 'premium', 'business']

# Generate subscription data for each user (you can adjust the number of subscriptions per user as needed)
products_data = [
    (
        user_id,  # Unique user_id
        random.choice(types_of_plans),  # Random subscription plan
        fake.date_between(start_date='-2y', end_date='today'),  # Random start date
        fake.date_between(start_date='today', end_date='+1y')   # Random end date
    )
    for user_id in user_ids[:100000]  # Limit to 100,000 subscriptions if there are enough user IDs
]

# Insert into subscriptions in chunks to avoid memory issues
chunk_size = 10000
for i in range(0, len(products_data), chunk_size):
    cursor.executemany(product_insert_query, products_data[i:i + chunk_size])
    connection.commit()
    print(f"Inserted {min(i + chunk_size, len(products_data))} rows into subscriptions.")

print("Inserted into subscriptions.")


# Insert 10,000,000 rows into opt_orders
print("Inserting into movies...")
order_insert_query = """
    INSERT INTO movies (title, year, genre, duration_minutes)
    VALUES (%s, %s, %s, %s)
"""

movie_titles = [
    'The Shawshank Redemption', 'The Godfather', 'The Dark Knight', 'Pulp Fiction',
    'Schindler\'s List', 'The Lord of the Rings: The Return of the King',
    'Fight Club', 'Forrest Gump', 'Inception', 'The Matrix',
    'The Empire Strikes Back', 'Interstellar', 'Parasite', 'The Green Mile',
    'Gladiator', 'The Silence of the Lambs', 'Saving Private Ryan',
    'The Prestige', 'The Departed', 'Whiplash',
    'The Lion King', 'The Avengers', 'Titanic', 'Jurassic Park',
    'The Godfather: Part II', 'Goodfellas', 'The Dark Knight Rises',
    'Avatar', 'Braveheart', 'Back to the Future'
]
genres = ['Action', 'Comedy', 'Drama', 'Horror', 'Sci-Fi', 'Romance', 'Thriller', 'Documentary']
order_date_start = datetime.now() - timedelta(days=365 * 5)
movies_data = [
    (
        random.choice(movie_titles),          # random movie title (3 words long)
        random.randint(1990, 2024),          # random year between 1950 and 2024
        random.choice(genres),               # random genre from the list
        random.randint(60, 180)
    )
    for _ in range(10000)
]

chunk_size = 10000
for i in range(0, len(movies_data), chunk_size):
    cursor.executemany(order_insert_query, movies_data[i:i + chunk_size])
    connection.commit()
    print(f"Inserted {i + chunk_size} rows into opt_orders...")

print("Inserted into movies.")

print("Inserting into watch_history...")
watch_history_insert_query = """
    INSERT INTO watch_history (user_id, movie_id, watch_date)
    VALUES (%s, %s, %s)
"""
user_ids = range(1, 100001)
movie_ids = range(1, 10001)

watch_history_data = [
    (
        random.choice(user_ids),            # random user_id
        random.choice(movie_ids),           # random movie_id
        fake.date_between(start_date='-2y', end_date='today')  # random watch_date in the past 2 years
    )
    for _ in range(100000)
]

chunk_size = 10000  # You can use chunking to insert data in smaller batches
for i in range(0, len(watch_history_data), chunk_size):
    cursor.executemany(watch_history_insert_query, watch_history_data[i:i + chunk_size])
    connection.commit()
    print(f"Inserted {min(i + chunk_size, len(watch_history_data))} rows into watch_history...")

print("Inserted into watch_history.")

# Close the cursor and connection
cursor.close()
connection.close()
