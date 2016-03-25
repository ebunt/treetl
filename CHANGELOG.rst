
v1.2.0
------

  * Add support for componentwise (extractor, transformer) construction of a job
  * Add ability to inject patches to jobs via `Job.inject`
  * Goal of inject is to allow for data fixes without changing the underlying job code and it's dependency tree


v1.1.0
------

  * Add dynamic job type creation and tests
  * Supported via `Job.create`
