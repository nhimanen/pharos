"""User domain model."""


class User:
    """Represents an application user with name and email."""

    def __init__(self, name, email):
        """Create a new User with the given name and email."""
        self.name = name
        self.email = email

    def get_name(self):
        """Return the user's display name."""
        return self.name

    def get_email(self):
        """Return the user's email address."""
        return self.email

    def has_valid_email(self):
        """Return True if the email address contains an @ sign."""
        return "@" in self.email
