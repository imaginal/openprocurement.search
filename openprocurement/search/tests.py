import unittest


class DummyTestCase(unittest.TestCase):
    """"""
    def setUp(self):
        self.client = client.Client('')

    def test_something(self):
        self.assertTrue(1 == 1)


if __name__ == '__main__':
    unittest.main()

