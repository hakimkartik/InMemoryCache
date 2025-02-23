from inMemoryCache import InMemoryCache


class InMemoryCacheImpl(InMemoryCache):
    DEFAULT_TTL = 1_00_000_000_000

    def __init__(self):
        self.data = {}
        self.timestamp_info = {}
        self.ttl_info = {}
        self.backup_info = {}

    def set(self, key, field, val):
        if key not in self.data:
            tmp = {field: val}
            self.data[key] = tmp
        else:
            self.data[key][field] = val

    def set_at(self, key, field, val, curr_timestamp):
        self.set(key, field, val)
        k = (key, field)
        self.timestamp_info[k] = curr_timestamp
        self.ttl_info[k] = InMemoryCacheImpl.DEFAULT_TTL
        self.check_and_delete_expired_records(curr_timestamp)

    def set_at_with_ttl(self, key, field, val, curr_timestamp, ttl):
        self.set(key, field, val)
        k = (key, field)
        self.timestamp_info[k] = curr_timestamp
        self.ttl_info[k] = ttl
        self.check_and_delete_expired_records(curr_timestamp)

    def get(self, key, field):
        res = None
        if key in self.data and field in self.data[key]:
            res = self.data[key][field]

        return res

    def get_at(self, key, field, curr_timestamp):
        res = None
        k = (key, field)
        self.check_and_delete_expired_records(curr_timestamp)
        if k in self.timestamp_info:
            created_at = self.timestamp_info[k]
            ttl = self.ttl_info[k]
            if created_at <= curr_timestamp < (created_at + ttl):
                res = self.get(key, field)

        return res

    def delete(self, key, field):
        res = False
        if key in self.data and field in self.data[key]:
            del self.data[key][field]
            res = True

        return res

    def delete_at(self, key, field, curr_timestamp):
        k = (key, field)
        res = False
        if k in self.timestamp_info:
            created_at = self.timestamp_info[k]
            ttl = self.ttl_info[k]
            if curr_timestamp >= (created_at + ttl):
                res = False
                self.delete(key, field)
            else:
                res = self.delete(key, field)

            del self.timestamp_info[k]
            del self.ttl_info[k]

        return res

    def scan(self, key):
        res = []
        if key in self.data:
            for field in self.data[key]:
                val = self.data[key][field]
                tmp = "{0}({1})".format(field, val)
                res.append(tmp)

            res.sort()

        return res

    def scan_at(self, key, curr_timestamp):
        res = []
        self.check_and_delete_expired_records(curr_timestamp)
        if key in self.data:
            for field in self.data[key]:
                k = (key, field)
                if k in self.timestamp_info:
                    created_at = self.timestamp_info[k]
                    ttl = self.ttl_info[k]

                    if created_at <= curr_timestamp < (created_at + ttl):
                        val = self.data[key][field]
                        tmp = "{0}({1})".format(field, val)
                        res.append(tmp)

            res.sort()

        return res

    def scan_by_prefix(self, key, prefix):
        res = []
        if key in self.data:
            for field in self.data[key]:
                if str(field).startswith(prefix):
                    val = self.data[key][field]
                    tmp = "{0}({1})".format(field, val)
                    res.append(tmp)

            res.sort()

        return res

    def scan_by_prefix_at(self, key, prefix, curr_timestamp):
        res = []
        self.check_and_delete_expired_records(curr_timestamp)
        if key in self.data:
            for field in self.data[key]:
                if str(field).startswith(prefix):
                    k = (key, field)
                    if k in self.timestamp_info:
                        created_at = self.timestamp_info[k]
                        ttl = self.ttl_info[k]
                        if created_at <= curr_timestamp < (created_at + ttl):
                            val = self.data[key][field]
                            tmp = "{0}({1})".format(field, val)
                            res.append(tmp)

        res.sort()
        return res

    def check_and_delete_expired_records(self, curr_timestamp):
        deletion = []
        print(f"...checking records at {curr_timestamp}")
        for key in self.data:
            for field in self.data[key]:
                k = (key, field)
                if k in self.timestamp_info:
                    created_at = self.timestamp_info[k]
                    expires_at = created_at + self.ttl_info[k]
                    if curr_timestamp >= expires_at:
                        print(f"...Gonna delete ({k}) as they expires_at: {expires_at} ")
                        deletion.append(k)

        for (k, f) in deletion:
            print("...Deleting : ({} , {})".format(k, f))
            del self.data[k][f]
            del self.timestamp_info[(k, f)]
            del self.ttl_info[(k, f)]
        print()

    def backup(self, curr_timestamp):
        """
        self.data = {}
        self.timestamp_info = {}
        self.ttl_info = {}
        self.backup_info = {}
        """
        self.check_and_delete_expired_records(curr_timestamp)
        self.backup_info[curr_timestamp] = self.data

        print("backup_info: ", self.backup_info)
        return len(self.backup_info)

    def restore(self, curr_timestamp):
        tmp = list(self.backup_info.keys())
        tmp.sort()
        print("tmp : ", tmp)
        if tmp[-1] <= curr_timestamp:
            ts = tmp[-1]
            self.data = self.backup_info[ts]
        else:
            print("curr_timestamp: ", curr_timestamp, " tmp[-1]: ", tmp[-1])

        self.check_and_delete_expired_records(curr_timestamp)