
import unittest


class TestJobCaching(unittest.TestCase):

    def check_event_order(self):
        def a_before_b(a, b):
            return self.event_sequence.index(a) < self.event_sequence.index(b)

        event_pairs = [
            ('JobA.transform', 'JobA.cache'), ('JobA.cache', 'JobB.transform'),
            ('JobB.transform', 'JobB.cache'), ('JobB.cache', 'JobD.transform'),
            ('JobA.cache', 'JobC.transform')
        ]
        for a, b in event_pairs:
            self.assertTrue(a in self.event_sequence, msg='Call {} not made'.format(a))
            self.assertTrue(b in self.event_sequence, msg='Call {} not made'.format(b))
            self.assertTrue(a_before_b(a, b))

        for c in [ 'JobC.cache', 'JobC.uncache', 'JobD.cache', 'JobD.uncache' ]:
            self.assertTrue(
                c not in self.event_sequence,
                msg='Erroneous {} call on {}.'.format(*c.split('.')[::-1])
            )

    def setUp(self):
        from treetl import Job

        self.event_sequence = [ ]

        def notify(job, msg=''):
            self.event_sequence.append(job.__class__.__name__ + msg)

        class NotifyingJob(Job):
            def transform(self, **kwargs):
                notify(self, '.transform')

            def cache(self, **kwargs):
                notify(self, '.cache')

            def uncache(self, **kwargs):
                notify(self, '.uncache')

        class JobA(NotifyingJob):
            def __init__(self):
                super(JobA, self).__init__()
                self.transformed_data = 0

            def transform(self, **kwargs):
                super(JobA, self).transform()
                self.transformed_data += 1

        @Job.dependency(a_data=JobA)
        class JobB(NotifyingJob):
           pass

        @Job.dependency(a_data=JobA)
        class JobC(NotifyingJob):
            pass

        @Job.dependency(b_dat=JobB)
        class JobD(NotifyingJob):
            pass

        self.jobs = [ JobB(), JobA(), JobC(), JobD() ]

    def test_job_caching(self):
        from treetl import JobRunner

        JobRunner(self.jobs).run()

        # check order of calls including cache calls
        # make sure no cache calls were made on nodes without children
        self.check_event_order()

        # checks 2 things
        #   1. ensure single transform call on root node
        #   2. also (by way of adding JobA second) implicitly checks that the implicitly created
        #      parent JobA gets replaced with the explicitly created
        self.assertTrue(self.jobs[1].transformed_data == 1, msg='JobA transformed more than once')
