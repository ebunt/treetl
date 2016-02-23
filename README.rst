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
    Job, job_dependency, 
    JobRunner, JOB_STATUS
  )

  class JobA(Job):
    pass

  # each of the methods in JobB can take a kwarg
  # that corresponds to JobA().transformed_data
  @job_dependency(a_param=JobA)
  class JobB(Job):
    pass

  @job_dependency(some_b_param=JobB)
  class JobC(Job):
    pass

  @job_dependency(input_param=JobA)
  class JobD(Job):
    pass

  @job_dependency(in_one=JobB, in_two=JobD)
  class JobE(Job):
    pass

  # order submitted doesn't matter
  jobs = JobRunner([ JobD(), JobC(), JobA(), JobB(), JobE() ])
  if jobs.run_all_jobs() == JOB_STATUS.FAILED:
    # to see this section in action add the following to
    # def transform(self): raise ValueError()
    # to the definition of JobD
    print('Jobs failed')
    print('Root jobs that caused the failure : {}'.format(jobs.failed_job_roots())
    print('Paths to sources of failure       : {}'.format(jobs.failed_job_root_paths())
  else:
    print('Success!')