treetl: Running data pipelines with tree-like dependencies
==========================================================

Pipelines don't need to be linear. Sometimes there are shared intermediate transformations that can feed future steps 
in the process. **treetl** manages and runs collections of dependent ETL jobs by storing and registering them
as a `polytree <https://en.wikipedia.org/wiki/Polytree>`_.

This package was put together with `Spark <http://spark.apache.org/>`_ jobs in mind, so caching intermediate and
carrying results forward is top of mind.

Example
=======

The following set of jobs will all run exactly once and pass their transformed data (or some reference to it) to the
jobs dependent upon them.

.. code:: python

  from treetl.jobs import (
    Job, JobRunner, JOB_STATUS
  )

  class JobA(Job):
    pass

  # each of the methods in JobB can take a kwarg named
  # a_param that corresponds to JobA().transformed_data
  @Job.dependency(a_param=JobA)
  class JobB(Job):
    pass

  @Job.dependency(some_b_param=JobB)
  class JobC(Job):
    pass

  @Job.dependency(input_param=JobA)
  class JobD(Job):
    pass

  @Job.dependency(in_one=JobB, in_two=JobD)
  class JobE(Job):
    def transform(self, in_one=None, in_two=None, **kwargs):
      # do stuff with in_one.transformed_data and in_two.transformed_data
      pass

  # order submitted doesn't matter
  jobs = JobRunner([ JobD(), JobC(), JobA(), JobB(), JobE() ])
  if jobs.run_all_jobs().status == JOB_STATUS.FAILED:
    # to see this section in action add the following to
    # def transform(self): raise ValueError()
    # to the definition of JobD
    print('Jobs failed')
    print('Root jobs that caused the failure : {}'.format(jobs.failed_job_roots())
    print('Paths to sources of failure       : {}'.format(jobs.failed_job_root_paths())
  else:
    print('Success!')


TODO
====

1. Implement `Job.create` factory method to dynamically create jobs with basic functions.
2. Set/pass state parameters to job methods
3. Job cloning with different parent jobs than original object so jobs can be reused in different places.
4. Support submitting a `JobRunner` as a job for nested job dependency graphs.
5. An `as_job` as either a mix-in or decorator for creating jobs out of other classes
