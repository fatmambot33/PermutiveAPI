# Identify Package

This package contains modules for managing user identification in the Permutive API. It allows you to associate users with various identifiers and send this information to Permutive.

## Modules

-   **`Identity.py`**: Provides the `Identity` class, which represents a user's profile. It holds a primary user ID and a list of aliases.
-   **`Alias.py`**: Provides the `Alias` class for representing user aliases. Aliases are used to link different identifiers (e.g., an email address, a device ID) to a single user profile.

## Usage Example

Here is a brief example of how to create a user identity with an alias and send it to the Permutive API.

```python
from PermutiveAPI import Identity, Alias

# Your private key for the Identify API
# Note: This is different from the regular API key
private_key = "your-private-key"

# Create an alias for the user
# This could be an email, a user ID from another system, etc.
user_alias = Alias(id="user@example.com", tag="email", priority=1)

# Create an identity for the user, associating them with the alias
user_identity = Identity(user_id="internal-user-id-abc-123", aliases=[user_alias])

# Send the identity information to Permutive
try:
    user_identity.identify(private_key=private_key)
    print(f"Successfully identified user: {user_identity.user_id}")
except Exception as e:
    print(f"An error occurred: {e}")
```

For more detailed examples, please refer to the main `README.md` in the root of the repository.
