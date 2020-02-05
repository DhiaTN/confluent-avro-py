class SerializerError(Exception):
    def __init__(self, error, message):
        self.error = str(error)
        self.message = message

    def __str__(self):
        return self.message
