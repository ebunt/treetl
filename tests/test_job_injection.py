
import unittest


class TestJobInjection(unittest.TestCase):

    def setUp(self):
        from treetl import Job, JobPatch

        class PatchJob(JobPatch):
            def __init__(self):
                self.some_patch_param = 10
                super(PatchJob, self).__init__()

            def some_other_method(self):
                return 2

            def extract(self, **kwargs):
                self.extracted_data.append(self.some_other_method())
                self.extracted_data = sorted(self.extracted_data)
                return self

            def transform(self, **kwargs):
                # oops also meant to scale it up
                self.transformed_data *= (2 + self.some_patch_param)
                return self

        # define the broken job and give it the necessary patch
        @Job.inject(PatchJob)
        class BaseJob(Job):
            def extract(self, **kwargs):
                self.extracted_data = [ 1, 3, 4 ]
                return self

            def transform(self, **kwargs):
                self.transformed_data = sum(self.extracted_data)
                return self

        self.patched_job = BaseJob()

    def test_job_injection(self):

        self.assertTrue(hasattr(self.patched_job, 'some_patch_param'), msg='Failed to port PatchJob.some_patch_param')
        self.assertTrue(hasattr(self.patched_job, 'some_other_method'), msg='Failed to port PatchJob.some_other_method')

        self.patched_job.extract().transform()
        self.assertEqual(self.patched_job.extracted_data, [ 1, 2, 3, 4 ], msg='Incorrect patched extracted_data')
        self.assertEqual(self.patched_job.transformed_data, 120, msg='Incorrect transformed data')

        self.patched_job.transformed_data = 0
        self.patched_job.some_patch_param = 5
        self.patched_job.extract().transform()
        self.assertEqual(self.patched_job.transformed_data, 70, msg='Incorrect transformed data')


if __name__ == '__main__':
    unittest.main()
