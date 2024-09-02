import unittest

from custom_cache import InMemoryCache


class TestCache(unittest.TestCase):

    def setUp(self) -> None:
        self.cache = InMemoryCache()

    def test_simple_set_get(self):
        self.cache.set("a", "b", 1)
        self.cache.set("a", "bc", 2)
        self.cache.set("a", "bb", 3)
        self.cache.set("a", "db", 4)
        self.cache.set("a", "dc", 5)
        self.assertEqual(self.cache.get("a", "b"), 1, "Didn't find '1' against field 'b' under 'a'")
        self.assertEqual(self.cache.get("a", "bc"), 2, "Didn't find 2 against field 'b' under 'a'")

    def test_simple_scan(self):
        self.cache.set("a", "b", 1)
        self.cache.set("a", "bc", 2)
        self.cache.set("a", "bb", 3)
        self.cache.set("a", "db", 4)
        self.cache.set("a", "dc", 5)
        expected_scan_res = ["b(1)", "bb(3)", "bc(2)", "db(4)", "dc(5)"]
        self.assertEqual(self.cache.scan("a"), expected_scan_res, "Scan result for key 'a' is incorrect")

    def test_simple_delete(self):
        self.cache.set("a", "b", 1)
        self.cache.set("a", "bc", 2)
        self.cache.set("a", "bb", 3)
        self.cache.set("a", "db", 4)
        self.cache.set("a", "dc", 5)
        self.assertTrue(self.cache.delete("a", "dc"))
        self.assertFalse(self.cache.delete("a", "d"))


if __name__ == '__main__':
    unittest.main()
