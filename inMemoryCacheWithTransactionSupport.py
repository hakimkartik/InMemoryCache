from inMemoryCache import InMemoryCache


class InMemoryCacheWithTransactionSupport(InMemoryCache):

    def __init__(self):
        self.data = {}
        self.transactions_stack = []

    def get(self, key, field):
        cache = self.data
        if bool(self.transactions_stack):
            cache = self.transactions_stack[-1]

        res = None
        if key in cache and field in cache[key]:
            res = cache[key][field]
        return res

    def set(self, key, field, val):
        if bool(self.transactions_stack):
            self.transactions_stack[-1][key] = {field: val}
        else:
            self.data[key] = {field : val}

    def _get_old_data(self):
        tmp = {}
        for k,v in self.data.items():
            tmp[k] = v
        return tmp

    def begin_transaction(self):
        self.transactions_stack.append(self._get_old_data())

    def commit(self):
        tmp = self.transactions_stack.pop()
        self.data = tmp

    def rollback(self):
        self.transactions_stack.pop()
