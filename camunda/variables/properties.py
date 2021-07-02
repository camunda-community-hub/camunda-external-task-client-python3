class Properties:
    """
    Properties are key->value pairs which acts as a kind of constant in Camunda.
    A property can be set via Camunda Modeller's Properties Panel on the Extension tab.

    Properties appear in a ExternalTask as soon as the config for an ExternalTaskClient will
    have a configuration includeExtensionProperties: True (which is the default)

    Properties will store strings only.
    """
    def __init__(self, properties={}):
        self.properties = properties

    def get_property(self, property_name) -> str:
        """
        access a single property
        """
        return self.properties.get(property_name, None)

    def to_dict(self) -> dict:
        """
        Converts the properties to a simple dictionary
        """
        result = {}
        for k, v in self.properties.items():
            result[k] = v
        return result
