
import unittest


class TestJobInjection(unittest.TestCase):

    def setUp(self):
        from treetl import Job
        from string import ascii_lowercase

        def extractor_one(x='a', y='b', **kwargs):
            raw_data = { lett: num+1 for num, lett in enumerate(ascii_lowercase) }
            return [ raw_data[x], raw_data[y] ]

        @Job.extractors(additional_extracted_data=extractor_one)
        class NewJob(Job):
            def extract(self, **kwargs):
                self.extracted_data = 3
                return self

            def transform(self, **kwargs):
                self.transformed_data = self.extracted_data * sum(self.additional_extracted_data)
                return self

        self.NewJob = NewJob

    def test_extractor_dec(self):
        self.assertEqual(self.NewJob().extract().transform().transformed_data, 9)
        self.assertEqual(self.NewJob().extract(y='d').transform().transformed_data, 15)
        self.assertEqual(self.NewJob().extract(x='e', y='d').transform().transformed_data, 27)


if __name__ == '__main__':
    unittest.main()
