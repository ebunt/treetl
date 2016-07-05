
import unittest


class TestJobExecutionOrder(unittest.TestCase):

    # can actually execute in a number of different ways
    # just need certain rules to hold with respect to the order
    # A before F, G
    # B, C before F
    # F before H, I
    # D before G before I
    def order_checks(self, exec_order):
        def chk(job_one, job_two):
            self.assertTrue(
                exec_order.index(job_one) < exec_order.index(job_two),
                msg='Out of order: {} supposed to be before {}'.format(job_one, job_two)
            )

        pairs_to_check = [
            ('JobA', 'JobF'), ('JobB', 'JobF'), ('JobC', 'JobF'), ('JobF', 'JobH'),
            ('JobF', 'JobI'), ('JobD', 'JobG'), ('JobG', 'JobI'), ('JobA', 'JobG')
        ]
        for job_one, job_two in pairs_to_check:
            chk(job_one, job_two)

    def setUp(self):
        from treetl import Job

        self.actual_execution_order = [ ]

        def notify(job):
            job_name = job.__class__.__name__
            self.actual_execution_order.append(job_name)

        class NotifyJob(Job):
            def transform(self, **kwargs):
                notify(self)
                return self

        class JobA(NotifyJob):
            pass

        class JobB(NotifyJob):
            pass

        class JobC(NotifyJob):
            pass

        class JobD(NotifyJob):
            pass

        class JobE(NotifyJob):
            pass

        @Job.dependency(b_data=JobB, c_data=JobC)
        class JobF(NotifyJob):
            pass

        @Job.dependency(a_data=JobA, d_data=JobD)
        class JobG(NotifyJob):
            pass

        @Job.dependency(a_data=JobA, f_data=JobF)
        class JobH(NotifyJob):
            pass

        @Job.dependency(f_data=JobF, g_data=JobG)
        class JobI(NotifyJob):
            pass

        @Job.dependency(f_data=JobF)
        class FaultyJob(NotifyJob):
            def transform(self, **kwargs):
                super(FaultyJob, self).transform()
                raise ValueError()

        @Job.dependency(faulty_parent=FaultyJob)
        class VictimJob(NotifyJob):
            pass

        @Job.dependency(faulty_parent=FaultyJob)
        class OtherVictimJob(NotifyJob):
            pass

        # they don't need to be in order
        self.jobs = [ JobA(), JobD(), JobC(), JobB(), JobE(), JobG(), JobF(), JobI(), JobH() ]
        self.faulty_jobs = [ FaultyJob(), VictimJob(), OtherVictimJob() ]

    def test_job_order(self):
        from treetl import Job, JobRunner, JOB_STATUS

        job_tree = JobRunner(self.jobs).run()

        self.order_checks(self.actual_execution_order)
        self.assertTrue(len(self.actual_execution_order) == len(self.jobs), msg='Some job transformed twice.')

        # add jobs that will fail and dependents that won't run as a result
        # clear execution order to start over again
        self.actual_execution_order = []

        job_tree.reset_jobs()
        job_tree.add_jobs(self.faulty_jobs)

        for failure_child in [ 'VictimJob', 'OtherVictimJob' ]:
            self.assertNotIn(
                member=failure_child,
                container=self.actual_execution_order,
                msg='Child of faulty, failed job was executed.'
            )

        self.assertTrue(job_tree.run().status == JOB_STATUS.FAILED, msg='Job failure not recorded in status')
        self.assertItemsEqual(
            expected_seq=self.faulty_jobs,
            actual_seq=job_tree.failed_jobs(),
            msg='Not all faulty jobs were labeled as failed.'
        )

        self.assertItemsEqual(
            expected_seq=[ self.faulty_jobs[0] ],
            actual_seq=job_tree.failed_job_roots(),
            msg='Root failure not correctly identified.'
        )

        failed_root_paths_dict = job_tree.failed_job_root_paths()
        self.assertTrue(len(failed_root_paths_dict) == 1, msg='Too many failure roots.')
        self.assertItemsEqual(
            expected_seq=[ self.faulty_jobs[0] ],
            actual_seq=failed_root_paths_dict.keys(),
            msg='Incorrect failed root in { failed_root: paths_to_failed_root }'
        )
        self.assertItemsEqual(
            expected_seq=[
                [ 'JobC', 'JobF', 'FaultyJob' ],
                [ 'JobB', 'JobF', 'FaultyJob' ]
            ],
            actual_seq=[
                [ job.__class__.__name__ for job in path ]
                for path in failed_root_paths_dict[self.faulty_jobs[0]]
            ],
            msg='Incorrect paths to root failure.'
        )
