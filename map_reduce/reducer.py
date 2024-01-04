#!/usr/bin/env python

import sys

current_key = None
current_values = []

for line in sys.stdin:
    try:
        # Parse the key-value pair
        key, value = line.strip().split('\t', 1)

        # Process data with the same key
        if current_key == key:
            current_values.append(value)
        else:
            # Add your reducer logic here
            # Example: Process data for a specific key
            print(f"Key: {current_key}, Values: {current_values}")

            # Reset for the new key
            current_key = key
            current_values = [value]

    except ValueError as e:
        # Handle value splitting errors
        print(f"Error splitting input: {e}", file=sys.stderr)

# Process the last key-value pair
if current_key is not None:
    print(f"Key: {current_key}, Values: {current_values}")
