
import unittest


class TestJobException(unittest.TestCase):

    def setUp(self):
        from treetl import Job, JobRunner

        class JobA(Job):
            def extract(self, **kwarg):
                raise ValueError()

        class JobB(Job):
            pass

        @Job.dependency(job_a=JobA, job_b=JobB)
        class JobC(Job):
            pass

        self.runner = JobRunner([ JobA(), JobB(), JobC() ])

    def test_exceptions(self):
        from treetl.job import JobException, ParentJobException

        self.runner.run()

        # run failed
        self.assertEqual(self.runner.run().status, 3)
        job_res = self.runner.job_results(submitted_only=True)

        # JobA is status 3 (Failed) and has JobException
        self.assertTrue(job_res[0].status == 3)
        self.assertIsInstance(job_res[0].error, JobException)

        # JobB may have been called first.
        # So it's either in the queue or done, but def no error
        self.assertTrue(job_res[1].status in [0, 2])
        self.assertTrue(job_res[1].error is None)

        # JobC has an error, but to no fault of its own
        # this one errored out bc parent job JobA had an error
        self.assertTrue(job_res[2].status == 3)
        self.assertIsInstance(job_res[2].error, ParentJobException)


if __name__ == '__main__':
    unittest.main()
