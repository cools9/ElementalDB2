import requests

# Base URL for the API
BASE_URL = "http://127.0.0.1:8000"  # Make sure the server is running at this address

def create_table():
    # Since the API does not have a create table endpoint yet,
    # we will assume the table is created at startup in the API.
    print("Table 'users' should be created on startup.")

def add_record(username, password, email):
    response = requests.post(f"{BASE_URL}/add/users", json={
        "username": username,
        "password": password,
        "email": email
    })
    print(response.json())

def get_record(username):
    response = requests.get(f"{BASE_URL}/get/users/{username}")
    if response.status_code == 200:
        print("Record found:", response.json())
    else:
        print("Error:", response.json())

def update_record(row_number, username, password, email):
    response = requests.put(f"{BASE_URL}/update/users/{row_number}", json={
        "username": username,
        "password": password,
        "email": email
    })
    print(response.json())

def delete_record(row_number):
    response = requests.delete(f"{BASE_URL}/delete/users/{row_number}")
    print(response.json())

if __name__ == "__main__":
    # 1. Create table (assumed to be done at startup)
    create_table()

    # 2. Add records
    add_record("john_doe", "securepass", "john@example.com")
    add_record("jane_doe", "mypassword", "jane@example.com")

    # 3. Get a record
    get_record("john_doe")  # Try to fetch the record of john_doe

    # 4. Update a record
    # Assuming john_doe is at row number 1 (you might need to adjust this based on actual records)
    update_record(1, "john_doe", "newsecurepass", "john_new@example.com")

    # 5. Delete a record
    delete_record(2)  # Delete the record of jane_doe
