import os
import orjson
import bcrypt
import asyncio
import time
from concurrent.futures import ProcessPoolExecutor
import aiofiles  # For async file I/O

class Table:
    def __init__(self, name, columns):
        self.name = name
        self.columns = columns
        self.records = []
        self.indexed_data = {col: {} for col in columns}

    def add_records(self, records):
        for record in records:
            if len(record) != len(self.columns):
                raise ValueError("Record does not match the number of defined columns.")
            record_dict = dict(zip(self.columns, record))
            self.records.append(record_dict)

            for col, value in record_dict.items():
                self.indexed_data[col].setdefault(value, []).append(len(self.records) - 1)

    def update(self, row_number, data):
        if not (1 <= row_number <= len(self.records)):
            raise ValueError("Row number out of range.")
        if len(data) != len(self.columns):
            raise ValueError("Data does not match the number of columns.")

        old_record = self.records[row_number - 1]
        for col, value in old_record.items():
            self.indexed_data[col][value].remove(row_number - 1)

        new_record = dict(zip(self.columns, data))
        self.records[row_number - 1] = new_record

        for col, value in new_record.items():
            self.indexed_data[col].setdefault(value, []).append(row_number - 1)

    def delete(self, row_number):
        if not (1 <= row_number <= len(self.records)):
            raise ValueError("Row number out of range.")

        record_to_delete = self.records.pop(row_number - 1)
        for col, value in record_to_delete.items():
            self.indexed_data[col][value].remove(row_number - 1)
            if not self.indexed_data[col][value]:
                del self.indexed_data[col][value]

        # Adjust the indices in the indexed data
        for col in self.indexed_data:
            for value in self.indexed_data[col]:
                self.indexed_data[col][value] = [i if i < row_number - 1 else i - 1 for i in self.indexed_data[col][value]]

    def search(self, value, in_columns):
        return [self.records[i] for col in in_columns for i in self.indexed_data.get(col, {}).get(value, [])]

    def to_dict(self):
        return {'columns': self.columns, 'records': self.records}

    def from_dict(self, data):
        self.columns = data['columns']
        self.records = data['records']
        self.indexed_data = {col: {} for col in self.columns}
        for index, record in enumerate(self.records):
            for col, value in record.items():
                self.indexed_data[col].setdefault(value, []).append(index)

class ElementalDB:
    def __init__(self, auth=False):
        self.tables = {}
        self.auth_enabled = auth
        self.auth_data_file = 'auth.auth'
        self.load_auth_data()
        self.current_user = None
        self.cache = {}

    def load_auth_data(self):
        if os.path.exists(self.auth_data_file):
            with open(self.auth_data_file, 'rb') as f:
                self.auth_data = orjson.loads(f.read())
        else:
            self.auth_data = {}

    def save_auth_data(self):
        with open(self.auth_data_file, 'wb') as f:
            f.write(orjson.dumps(self.auth_data))

    async def signup(self, email, password):
        if email in self.auth_data:
            raise ValueError("Email already exists.")

        hashed_password = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())
        self.auth_data[email] = hashed_password.decode('utf-8')
        self.save_auth_data()
        print("Signup successful!")

    async def signin(self, email, password):
        if email not in self.auth_data:
            raise ValueError("Email does not exist.")

        hashed_password = self.auth_data[email].encode('utf-8')
        if bcrypt.checkpw(password.encode('utf-8'), hashed_password):
            self.current_user = email
            print("Signin successful!")
        else:
            raise ValueError("Invalid password.")

    def logout(self):
        if self.current_user:
            print(f"User '{self.current_user}' logged out.")
            self.current_user = None
        else:
            print("No user is currently signed in.")

    def table_create(self, name, columns, overwrite=False):
        if self.auth_enabled and not self.current_user:
            raise PermissionError("You must be signed in to access the database.")
        if name in self.tables and not overwrite:
            raise ValueError(f"Table '{name}' already exists. Use 'overwrite=True' to overwrite.")
        self.tables[name] = Table(name, columns)
        return self.tables[name]

    async def save_table(self, table_name):
        if table_name in self.tables:
            table = self.tables[table_name]
            file_path = f'database/{table_name}.json'
            if not os.path.exists('database'):
                os.makedirs('database')
            async with aiofiles.open(file_path, 'wb') as f:
                json_data = orjson.dumps(table.to_dict())
                await f.write(json_data)

    async def load_table(self, table_name):
        file_path = f'database/{table_name}.json'
        if os.path.exists(file_path):
            async with aiofiles.open(file_path, 'rb') as f:
                data = orjson.loads(await f.read())
                table = Table(table_name, data['columns'])
                table.from_dict(data)
                self.tables[table_name] = table
        else:
            raise ValueError(f"Table '{table_name}' does not exist.")

    async def add(self, table_name, records):
        if self.auth_enabled and not self.current_user:
            raise PermissionError("You must be signed in to add records.")
        if table_name not in self.tables:
            raise ValueError(f"Table '{table_name}' does not exist.")
        table = self.tables[table_name]
        table.add_records(records)
        await self.save_table(table_name)

    async def update(self, table_name, row_number, data):
        if self.auth_enabled and not self.current_user:
            raise PermissionError("You must be signed in to update records.")
        if table_name not in self.tables:
            raise ValueError(f"Table '{table_name}' does not exist.")
        table = self.tables[table_name]

        with ProcessPoolExecutor() as executor:
            await asyncio.get_event_loop().run_in_executor(executor, table.update, row_number, data)

        await self.save_table(table_name)

    async def delete(self, table_name, row_number):
        if self.auth_enabled and not self.current_user:
            raise PermissionError("You must be signed in to delete records.")
        if table_name not in self.tables:
            raise ValueError(f"Table '{table_name}' does not exist.")
        table = self.tables[table_name]

        with ProcessPoolExecutor() as executor:
            await asyncio.get_event_loop().run_in_executor(executor, table.delete, row_number)

        await self.save_table(table_name)

    async def search(self, table_name, value, in_columns):
        # Check cache first
        cache_key = (table_name, value, tuple(in_columns))
        if cache_key in self.cache:
            return self.cache[cache_key]  # Return from cache if exists

        if table_name not in self.tables:
            raise ValueError(f"Table '{table_name}' does not exist.")
        table = self.tables[table_name]
        results = table.search(value, in_columns)
        self.cache[cache_key] = results  # Cache the results for future queries
        return results

    def list_tables(self):
        return list(self.tables.keys())

# Example usage for creating, adding, updating, searching, deleting records
async def main():
    start = time.time()
    db = ElementalDB(auth=True)

    # User signup and signin
    try:
        await db.signin('user@example.com', 'password123')
    except Exception as e:
        print(e)

    # Create a table (no await since it's a synchronous method)
    db.table_create('Users', ['id', 'name', 'age'])

    # Add records
    await db.add('Users', [(1, 'Alice', 30), (2, 'Bob', 25)])

    # Update a record
    await db.update('Users', 1, (1, 'Alice', 31))

    # Search for a record
    results = await db.search('Users', 'Alice', ['name'])
    print(f"Search results: {results}")

    # Delete a record
    await db.delete('Users', 1)

    end = time.time()
    print(f"Operation completed in {end - start:.4f} seconds")

asyncio.run(main())  # Uncomment this line to run the example usage
