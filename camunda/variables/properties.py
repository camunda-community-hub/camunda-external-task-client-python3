class Properties:
    def __init__(self, properties={}):
        self.properties = properties

    def get_property(self, property_name):
        prop = self.properties.get(property_name, None)
        if not prop:
            return None

        return prop["value"]

    def to_dict(self):
        """
        Converts the properties to a simple dictionary
        :return: dict
            {"var1": {"value": 1}, "var2": {"value": True}}
            ->
            {"var1": 1, "var2": True}
        """
        result = {}
        for k, v in self.properties.items():
            result[k] = v
        return result
