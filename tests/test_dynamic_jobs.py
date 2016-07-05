
import unittest


class TestDynamicJobCreation(unittest.TestCase):
    def setUp(self):

        self.extract_ans = 1
        self.transformed_ans = 2
        self.load_ans = 3
        self.cache_ans = 4
        self.uncache_ans = 5

        self.load_ans_dest = None
        self.cache_ans_dest = None
        self.uncache_ans_dest = None

        # all the methods that will get wrapped and return self
        def extract(**kwargs):
            return 1

        def transform(extracted_data=None, parent_data=0, **kwargs):
            return 1 + extracted_data + parent_data

        def load(transformed_data=None, **kwargs):
            self.load_ans_dest = 1 + transformed_data

        def cache(transformed_data=None, **kwargs):
            self.cache_ans_dest = 2 + transformed_data

        def uncache(transformed_data=None, **kwargs):
            self.uncache_ans_dest = 3 + transformed_data

        self.extract = extract
        self.transform = transform
        self.load = load
        self.cache = cache
        self.uncache = uncache

    def test_dyn_jobs(self):
        from treetl import Job

        def make_job(**kwargs):
            return Job.create(
                'DynJob',
                extract=self.extract,
                transform=self.transform,
                load=self.load,
                cache=self.cache,
                uncache=self.uncache,
                **kwargs
            )

        def run_test(inc, test_type, job):
            self.assertEqual(
                job.extracted_data,
                self.extract_ans,
                msg='{}: Incorrect extracted data'.format(test_type)
            )
            self.assertEqual(
                job.transformed_data,
                self.transformed_ans + inc,
                msg='{}: Incorrect transformed data'.format(test_type)
            )

            self.assertEqual(
                self.load_ans_dest,
                self.load_ans + inc,
                msg='{}: Incorrect load answer data'.format(test_type)
            )

            self.assertEqual(
                self.cache_ans_dest,
                self.cache_ans + inc,
                msg='{}: Incorrect cache answer data'.format(test_type)
            )

            self.assertEqual(
                self.uncache_ans_dest,
                self.uncache_ans + inc,
                msg='{}: Incorrect uncache answer data'.format(test_type)
            )

        # run without dependent jobs
        job = make_job()().extract().transform().load().cache().uncache()
        run_test(0, 'No Parent Job', job)

        # test job dependency
        class Parent(Job):
            def transform(self, **kwargs):
                self.transformed_data = 3
                return self

        # run with dependent jobs
        from treetl import JobRunner

        dyn_job = make_job(parent_data=Parent)()
        JobRunner(jobs=[ dyn_job ]).run()
        dyn_job.cache().uncache()  # only one parent, so won't be called by JobRunner
        run_test(3, 'With Parent Job', dyn_job)


if __name__ == '__main__':
    unittest.main()
