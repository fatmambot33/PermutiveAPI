# Audience Package

This package contains modules for managing audience-related resources in the Permutive API. These resources are typically used to define and manage groups of users based on their behavior or other characteristics.

## Modules

-   **`Import.py`**: Provides the `Import` and `ImportList` classes for managing data imports. Imports are used to bring external data into Permutive.
-   **`Segment.py`**: Provides the `Segment` and `SegmentList` classes for managing audience segments. Segments are often created based on data from an `Import`.
-   **`Source.py`**: Provides the `Source` class, which represents the source of an `Import` (e.g., a specific file or data stream).

## Usage Example

Here is a brief example of how to list imports and then view the segments associated with a particular import.

```python
from PermutiveAPI import Import, Segment

# Your API key
api_key = "your-api-key"

# List all available imports
try:
    all_imports = Import.list(api_key=api_key)
    print(f"Found {len(all_imports)} imports.")

    if all_imports:
        # Get the ID of the first import
        first_import_id = all_imports[0].id
        print(f"Listing segments for import: {first_import_id}")

        # List all segments for that import
        segments = Segment.list(api_key=api_key, import_id=first_import_id)
        for segment in segments:
            print(f"- Segment ID: {segment.id}, Name: {segment.name}")

except Exception as e:
    print(f"An error occurred: {e}")
```

For more detailed examples, please refer to the main `README.md` in the root of the repository.
