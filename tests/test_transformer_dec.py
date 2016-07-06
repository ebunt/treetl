
import unittest


class TestJobInjection(unittest.TestCase):

    def setUp(self):
        from treetl import Job

        def transformer_one(data, **kwargs):
            return sum(data)

        def transformer_two(data, scale=3, **kwargs):
            return data * scale

        @Job.transformers(transformer_one, transformer_two)
        class NewJob(Job):
            def extract(self, **kwargs):
                self.extracted_data = [ 1, 2, 3 ]
                return self

        self.NewJob = NewJob

    def test_extractor_dec(self):
        self.assertEqual(self.NewJob().extract().transform().transformed_data, 18)
        self.assertEqual(self.NewJob().extract().transform(scale=5).transformed_data, 30)


if __name__ == '__main__':
    unittest.main()
