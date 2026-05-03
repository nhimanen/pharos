"""Greeting service that produces personalized messages."""

from model.user import User


class GreetingService:
    """Produces greeting and farewell messages for users."""

    def greet(self, user):
        """Return a friendly greeting for the given user."""
        name = user.get_name()
        return f"Hello, {name}!"

    def farewell(self, user):
        """Return a farewell message for the given user."""
        name = user.get_name()
        return f"Goodbye, {name}!"

    def formal_greet(self, user):
        """Return a formal greeting using the user's email domain."""
        name = user.get_name()
        email = user.get_email()
        domain = email.split("@")[-1] if user.has_valid_email() else "unknown"
        return f"Good day, {name} of {domain}."
