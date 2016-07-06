

import unittest
import logging

logger = logging.getLogger()
logger.level = logging.DEBUG


class TestLogging(unittest.TestCase):
    def set_up_handler(self):
        from treetl.tools.testing import MockLoggingHandler

        self.handler = MockLoggingHandler(level='DEBUG')
        logger.addHandler(self.handler)

    def set_up_jobs(self):
        from treetl import Job, JobRunner

        class JobA(Job):
            pass

        class JobB(Job):
            pass

        @Job.dependency(a=JobA, b=JobB)
        class JobC(Job):
            def transform(self, **kwargs):
                raise ValueError()

        @Job.dependency(c=JobC)
        class JobD(Job):
            def load(self, **kwargs):
                # will never get here
                raise ValueError()

        self.runner = JobRunner([ JobC(), JobD() ])

    def setUp(self):
        self.set_up_handler()
        self.set_up_jobs()

    def jobs_and_parents_were_added(self, debug_msgs):
        self.assertIn('JobRunner: Adding job JobC with 2 parent(s)', debug_msgs, msg='debug msg err: JobC add')
        self.assertIn('JobRunner: Adding job JobD with 1 parent(s)', debug_msgs, msg='debug msg err: JobD add')

    def successful_steps_were_in_debug(self, debug_msgs):
        # make sure all successful job steps made it to debug
        # JobA and JobB should run fully, JobC extract only, JobD not at all
        for m in [
            'JobA.extract()', 'JobA.transform()', 'JobA.load()',
            'JobB.extract()', 'JobB.transform()', 'JobB.load()', 'JobC.extract()'
        ]:
            self.assertIn(m, debug_msgs, msg='debug msg err: ' + m)

    def cached_jobs_in_debug(self, debug_msgs):
        # jobs A and B should be cached since they'll be called upon by C
        for j in ['A', 'B']:
            self.assertIn(
                'Job{}.cache() | children:1'.format(j),
                debug_msgs,
                msg='debug msg err: Job{}.cache()'.format(j)
            )

    def correct_transform_signature_in_debug(self, debug_msgs):
        err_msg = 'debug msg err: JobC transform signature is incorrect or missing from debug'
        self.assertIn('JobC.transform(a=None, b=None)', debug_msgs, msg=err_msg)

    def job_runner_job_status(self, info_msgs):
        err_msg = 'info msg err: did not start with RUNNING status'
        self.assertEqual('JobRunner: JOB_STATUS.RUNNING', info_msgs[0], msg=err_msg)

        err_msg = 'info msg err: did not end with FAILED status'
        self.assertEqual('JobRunner: JOB_STATUS.FAILED', info_msgs[-1], msg=err_msg)

    def check_info_for_job_start_attempts(self, info_msgs):
        err_msg = 'info msg err: Failed to note start of Job'
        # should start and try to run jobs A, B and C.
        # D should be skipped since C fails
        for j in ['A', 'B', 'C']:
            self.assertIn('JobRunner: Running Job' + j, info_msgs, msg=err_msg + j)

    def check_info_for_completed_jobs(self, info_msgs):
        err_msg = 'info msg err: Failed to note competion of Job'
        # jobs A and B should finish.
        for j in ['A', 'B']:
            self.assertIn('JobRunner: Completed Job' + j, info_msgs, msg=err_msg + j)

    def info_has_skipped_job(self, info_msgs):
        self.assertIn(
            'JobRunner: Skipped JobD due to failure in parent JobC',
            info_msgs,
            msg='info msg err: failed to note skipped JobD due to JobC failure'
        )

    def error_has_job_failure(self, err_msgs):
        self.assertIn(
            'JobRunner: Error on JobC',
            err_msgs,
            msg='err msg err: failed to note JobC failure'
        )

    def test_job_logging(self):
        self.runner.run()

        debug_msgs = self.handler.messages['debug']
        self.jobs_and_parents_were_added(debug_msgs)
        self.successful_steps_were_in_debug(debug_msgs)
        self.cached_jobs_in_debug(debug_msgs)

        info_msgs = self.handler.messages['info']
        self.job_runner_job_status(info_msgs)
        self.check_info_for_job_start_attempts(info_msgs)
        self.check_info_for_completed_jobs(info_msgs)

        self.error_has_job_failure(self.handler.messages['error'])

    def tearDown(self):
        logger.removeHandler(self.handler)


if __name__ == '__main__':
    unittest.main()
