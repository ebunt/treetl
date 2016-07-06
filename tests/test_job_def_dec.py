
import unittest


class TestJobDependencyDecorator(unittest.TestCase):

    def test_job_dep_dec(self):
        from treetl import Job

        class JobA(Job):
            pass

        @Job.dependency(a_data=JobA)
        class JobB(Job):
            pass

        self.assertEqual(first={}, second=JobA.ETL_SIGNATURE)
        self.assertEqual(first={ 'a_data': JobA }, second=JobB.ETL_SIGNATURE)


if __name__ == '__main__':
    unittest.main()
