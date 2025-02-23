from abc import ABC


class InMemoryCache(ABC):
    def set(self, key, field, val):
        pass

    def set_at(self, key, field, val, curr_timestamp):
        pass

    def delete(self, key, field):
        pass

    def delete_at(self, key, field, curr_timestamp):
        pass

    def get(self, key, field):
        pass

    def get_at(self, key, field, curr_timestamp):
        pass

    def backup(self, curr_timestamp):
        pass

    def restore(self, curr_timestamp):
        pass
