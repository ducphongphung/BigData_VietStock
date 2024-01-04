#!/usr/bin/env python

import sys
import json

for line in sys.stdin:
    try:
        # Parse the JSON message
        data = json.loads(line.strip())

        # Add your mapper logic here
        # Example: Emit key-value pairs
        for key, value in data.items():
            print(f"{key}\t{value}")

    except json.JSONDecodeError as e:
        # Handle JSON decoding errors
        print(f"Error decoding JSON: {e}", file=sys.stderr)
# !/usr/bin/env python

import sys
import json

for line in sys.stdin:
    try:
        # Parse the JSON message
        data = json.loads(line.strip())

        # Add your mapper logic here
        # Example: Emit key-value pairs
        for key, value in data.items():
            print(f"{key}\t{value}")

    except json.JSONDecodeError as e:
        # Handle JSON decoding errors
        print(f"Error decoding JSON: {e}", file=sys.stderr)
