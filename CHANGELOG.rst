
v1.2.1
------

  * Fix `Job.inject` bug that lead to infinite `super().__init__` recursion
  * Injection is gone by appending all non ETL-CU attributes and methods to decorated job
  * The main ETL-CU patches are made static and embedded in the base job's corresponding method
  * All patches must now either inherit from `jobs.JobPatch` or set metaclass to `jobs.JobPatchMeta`


v1.2.0
------

  * Add support for componentwise (extractor, transformer) construction of a job
  * Add ability to inject patches to jobs via `Job.inject`
  * Goal of inject is to allow for data fixes without changing the underlying job code and it's dependency tree


v1.1.0
------

  * Add dynamic job type creation and tests
  * Supported via `Job.create`
