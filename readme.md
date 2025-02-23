# In-Memory Cache Implementation

## Overview
This project provides an in-memory key-field-value cache with timestamp-based operations and optional transaction support. It consists of an abstract base class (`InMemoryCache`), a default implementation (`InMemoryCacheImpl`), and an extended implementation with transaction support (`InMemoryCacheWithTransactionSupport`).

## Files
- **`inMemoryCache.py`**: Defines an abstract base class `InMemoryCache` that outlines the structure of the cache operations.
- **`custom_cache.py`**: Implements the `InMemoryCacheImpl` class, which provides a concrete implementation of the cache, including expiration handling and backup functionality.
- **`inMemoryCacheWithTransactionSupport.py`**: Implements `InMemoryCacheWithTransactionSupport`, which extends the base cache with transaction support, allowing rollback and commit operations.

## Features
- **Basic CRUD Operations**:
  - `set(key, field, value)`: Store a value in the cache.
  - `get(key, field)`: Retrieve a value from the cache.
  - `delete(key, field)`: Remove a value from the cache.
- **Timestamp-Based Operations**:
  - `set_at(key, field, value, timestamp)`: Set a value with an associated timestamp.
  - `get_at(key, field, timestamp)`: Retrieve a value based on the timestamp.
  - `delete_at(key, field, timestamp)`: Delete a value based on expiration.
- **Time-to-Live (TTL) Support**:
  - `set_at_with_ttl(key, field, value, timestamp, ttl)`: Set a value with an explicit TTL.
- **Scanning and Filtering**:
  - `scan(key)`: Retrieve all key-field-value pairs for a given key.
  - `scan_at(key, timestamp)`: Retrieve valid key-field-value pairs at a given timestamp.
  - `scan_by_prefix(key, prefix)`: Retrieve fields that match a specific prefix.
  - `scan_by_prefix_at(key, prefix, timestamp)`: Retrieve timestamp-based field values matching a prefix.
- **Backup and Restore**:
  - `backup(timestamp)`: Create a backup of the cache at a given timestamp.
  - `restore(timestamp)`: Restore the cache from the latest backup before the given timestamp.
- **Automatic Expiration Handling**:
  - `check_and_delete_expired_records(timestamp)`: Automatically removes expired records based on TTL settings.
- **Transaction Support**:
  - `begin_transaction()`: Begins a new transaction, storing the current state.
  - `commit()`: Commits the transaction, making changes permanent.
  - `rollback()`: Rolls back to the last saved state, undoing changes within a transaction.

## Usage
The cache can be tested using the provided `case1()`, `case2()`, and `case3()` functions in `custom_cache.py`. Running the script executes `case3()` by default.

### Running the script
```bash
  python custom_cache.py
```

### Example Usage
```python
from custom_cache import InMemoryCacheImpl

cache = InMemoryCacheImpl()
cache.set("user1", "name", "Alice")
print(cache.get("user1", "name"))  # Output: Alice

cache.set_at_with_ttl("session", "token", "abc123", 100, 5)
print(cache.get_at("session", "token", 102))  # Output: abc123
print(cache.get_at("session", "token", 106))  # Output: None (expired)
```

### Transaction Example
```python
from inMemoryCacheWithTransactionSupport import InMemoryCacheWithTransactionSupport

cache = InMemoryCacheWithTransactionSupport()
cache.set("user1", "name", "Alice")
cache.begin_transaction()
cache.set("user1", "name", "Bob")
print(cache.get("user1", "name"))  # Output: Bob
cache.rollback()
print(cache.get("user1", "name"))  # Output: Alice
```

## Requirements
- Python 3.x
