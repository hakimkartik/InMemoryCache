from inMemoryCacheImpl import InMemoryCacheImpl


def populate_with_data(obj):
    obj.set("a", "b", 1)
    obj.set("a", "bc", 2)
    obj.set("a", "bb", 3)
    obj.set_at_with_ttl("b", "a", 1, 18, 5)
    obj.set_at_with_ttl("b", "ad", 3, 16, 6)
    obj.set_at("a", "b", 1, 10)
    obj.set_at_with_ttl("a", "bd", 4, 15, 10)
    obj.set_at("a", "bc", 2, 11)
    obj.set_at("a", "bb", 3, 15)
    obj.set_at_with_ttl("a", "v", 42, 16, 10)
    obj.set_at_with_ttl("a", "vb", 42, 17, 8)
    obj.set_at_with_ttl("a", "vc", 42, 19, 15)


def case3():
    obj = InMemoryCacheImpl()
    populate_with_data(obj)
    print("data: ", obj.data)
    print("ttl: ", obj.ttl_info)
    print("timestamps: ", obj.timestamp_info)
    print("backup: ", obj.backup(5))


def case2():
    obj = InMemoryCacheImpl()
    populate_with_data(obj)
    print("data: ", obj.data)
    print("ttl: ", obj.ttl_info)
    print("timestamps: ", obj.timestamp_info)
    print("get_at(a, b, 19): ", obj.get_at("a", "b", 19))
    print("get_at(a, bd, 20): ", obj.get_at("a", "bd", 20))
    print("get_at(b, a, 21): ", obj.get_at("b", "a", 21))
    print("get_at(b, ad, 22): ", obj.get_at("b", "ad", 22))
    print("data: ", obj.data)
    print("ttl: ", obj.ttl_info)
    print("scan_at(a, 24): ", obj.scan_at("a", 24))
    print("scan_at(a, 30): ", obj.scan_at("a", 30))


def case1():
    obj = InMemoryCacheImpl()
    populate_with_data(obj)
    print("get(a, b): ", obj.get("a", "b"))
    print("get(a, bb): ", obj.get("a", "bb"))
    print("get(a, c): ", obj.get("a", "c"))
    print("scan(a): ", obj.scan("a"))
    print("scan(b): ", obj.scan("b"))
    print("data: ", obj.data)
    print("delete(b, a): ", obj.delete("b", "a"))
    print("delete(a, b): ", obj.delete("a", "b"))
    print("data: ", obj.data)


if __name__ == "__main__":
    case3()
