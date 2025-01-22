class classproperty(property):
    """
    Utility descriptor, 'cause classmethod and property can no longer be chained:
    https://docs.python.org/3.13/library/functions.html#classmethod
    ..but it was cool.
    """

    def __get__(self, owner_self, owner_cls):
        return self.fget(owner_cls)
