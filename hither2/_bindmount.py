class BindMount:
    def __init__(self, source: str, target: str, read_only: bool):
        self.source = source
        self.target = target
        self.read_only = read_only
    def serialize(self):
        return {
            'source': self.source,
            'target': self.target,
            'read_only': self.read_only
        }
    @staticmethod
    def deserialize(x: dict):
        return BindMount(**x)
