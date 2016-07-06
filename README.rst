treetl: Running ETL tasks with tree-like dependencies
=====================================================

Pipelines of batch jobs don't need to be linear. Sometimes there are shared intermediate transformations that can feed
future steps in the process. **treetl** manages and runs collections of dependent ETL jobs by storing and registering
them as a `polytree <https://en.wikipedia.org/wiki/Polytree>`_.

This package was put together with `Spark <http://spark.apache.org/>`_ jobs in mind, so caching intermediate and
carrying results forward is top of mind. Due to this, one of the main benefits of **treetl** is that partial
job results can be shared in memory.

Example
=======

The following set of jobs will all run exactly once and pass their transformed data (or some reference to it) to the
jobs dependent upon them.

.. code:: python

  from treetl import (
    Job, JobRunner, JOB_STATUS
  )

  class JobA(Job):
    def transform(self, **kwargs):
      self.transformed_data = 1
      return self

  # JobB.transform can take a kwarg named
  # a_param that corresponds to JobA().transformed_data
  @Job.dependency(a_param=JobA)
  class JobB(Job):
    def transform(self, a_param=None, **kwargs):
      self.transformed_data = a_param + 1
      return self

    def load(self, **kwargs):
      # could save intermediate result self.transformed_data here
      pass

  @Job.dependency(some_b_param=JobB)
  class JobC(Job):
    pass

  @Job.dependency(input_param=JobA)
  class JobD(Job):
    def transform(self, input_param=None, **kwargs):
      self.transformed_data = input_param + 1
      return self

  @Job.dependency(in_one=JobB, in_two=JobD)
  class JobE(Job):
    def transform(self, in_one=None, in_two=None, **kwargs):
      # do stuff with in_one.transformed_data and in_two.transformed_data
      self.transformed_data = in_one + in_two

  # order submitted doesn't matter
  jobs = [ JobD(), JobC(), JobA(), JobB(), JobE() ]
  job_runner = JobRunner(jobs)
  if job_runner.run().status == JOB_STATUS.FAILED:
    # to see this section in action add the following to
    # def transform(self): raise ValueError()
    # to the definition of JobD
    print('Jobs failed')
    print('Root jobs that caused the failure : {}'.format(job_runner.failed_job_roots()))
    print('Paths to sources of failure       : {}'.format(job_runner.failed_job_root_paths()))
  else:
    print('Success!')
    print('JobE transformed data: {}'.format(jobs[4].transformed_data))


TODO
====

* Set parameters common to multiple jobs via the top level JobRunner
* Set/pass state parameters to job methods
* Support submitting a `JobRunner` as a job for nested job dependency graphs.
* Run from a specific point in the tree. Allow for parents of starting point to retrieve last loaded data instead of recomputing the whole set of dependencies.
* Ability to pass job attributes to component functions used in the decorator based definition of a job
