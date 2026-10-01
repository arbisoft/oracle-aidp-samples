"""Offline stand-ins for the bits of PySpark/Py4J that mongodb_client.py touches. No network, no Spark."""


class FakeSchema:
    def __init__(self, json_value):
        self.json_value = json_value

    def jsonValue(self):
        return self.json_value

    @classmethod
    def fromJson(cls, json_value):
        return cls(json_value)


class FakeDF:
    def __init__(self, schema):
        self.schema = schema


class FakeReader:
    def __init__(self, spark):
        self.spark = spark
        self.options = {}
        self.fmt = None
        self.explicit_schema = None

    def format(self, fmt):
        self.fmt = fmt
        return self

    def option(self, key, value):
        self.options[key] = value
        return self

    def schema(self, schema):
        self.explicit_schema = schema
        return self

    def load(self):
        self.spark.loads.append(self)
        if self.spark.error is not None:
            raise self.spark.error
        return FakeDF(self.explicit_schema or FakeSchema(self.spark.inferred))


class FakeSpark:
    """``spark.read`` returns a fresh reader each time, like the real one."""

    def __init__(self, inferred=None, error=None):
        self.inferred = inferred or {"type": "struct", "fields": []}
        self.error = error
        self.loads = []

    @property
    def read(self):
        return FakeReader(self)


class FakeJavaException:
    def __init__(self, text, cause=None):
        self.text = text
        self.cause = cause

    def getCause(self):
        return self.cause

    def toString(self):
        return self.text


class FakePy4JError(Exception):
    """Shaped like py4j.protocol.Py4JJavaError: a useless str(), the real
    cause chain under ``java_exception``."""

    def __init__(self, java_exception):
        super().__init__("An error occurred while calling o78.load.")
        self.java_exception = java_exception


def field(name, type_, nullable=False):
    return {"name": name, "type": type_, "nullable": nullable, "metadata": {}}
