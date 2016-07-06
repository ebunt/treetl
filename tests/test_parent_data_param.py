
import unittest


class TestParentDataParams(unittest.TestCase):

    def setUp(self):
        from treetl import Job

        self.expected_results = { jn: i+1 for i, jn in enumerate([ 'JobA', 'JobB', 'JobC', 'JobD' ]) }
        self.actual_results = { }

        def update_actual_results(job):
            self.actual_results[job.__class__.__name__] = job.transformed_data

        class LoadToDict(Job):
            def load(self, **kwargs):
                update_actual_results(self)

        class JobA(LoadToDict):
            def transform(self, **kwargs):
                self.transformed_data = 1

        class JobB(LoadToDict):
            def transform(self, **kwargs):
                self.transformed_data = 2

        @Job.dependency(b_data=JobB, a_data=JobA)
        class JobC(LoadToDict):
            def transform(self, a_data=None, b_data=None, **kwargs):
                self.transformed_data = a_data + b_data


        @Job.dependency(a_data=JobA, c_data=JobC)
        class JobD(LoadToDict):
            def transform(self, a_data=None, c_data=None, **kwargs):
                self.transformed_data = a_data + c_data

        self.jobs = [ JobD(), JobA(), JobC(), JobB() ]

    def test_parent_data_params(self):
        from treetl import JobRunner

        JobRunner(self.jobs).run()
        self.assertDictEqual(
            d1=self.expected_results,
            d2=self.actual_results,
            msg='Error in transformed data loaded to dict'
        )

if __name__ == '__main__':
    unittest.main()
