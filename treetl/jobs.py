
from treetl.tools import build_enum
from treetl.polytree import PolyTree, TreeNode


JOB_STATUS = build_enum('QUEUE', 'RUNNING', 'DONE', 'FAILED')


_job_methods = [ 'extract', 'transform', 'load', 'cache', 'uncache' ]


class JobPatchMeta(type):
    """
    Metaclass for job patches that creates static version of ETL-CU methods. The static methods are named
    `static_[method]` and plugged into the corresponding method in the job being patched.
    """
    def __new__(cls, name, bases, dict):
        for m in _job_methods:
            if m in dict:
                dict['static_' + m] = staticmethod(dict[m])

        return super(JobPatchMeta, cls).__new__(cls, name, bases, dict)


# defined this way instead of with metaclass hooks for 2.x and 3.x portability
JobPatch = JobPatchMeta(str('JobPatch'), (), {})


class Job(object):

    # store the proper param names for transformed
    # data from parent jobs. populated by decorator
    ETL_SIGNATURE = { }

    # add this decorator to populate ETL_SIGNATURE (in a nice looking way)
    @staticmethod
    def dependency(**kwargs):
        def class_wrap(cls):
            cls.ETL_SIGNATURE = {
                param_name: job_type
                for param_name, job_type in kwargs.items()
            } if kwargs else { }
            return cls
        return class_wrap

    @staticmethod
    def inject(*args):
        """
        Apply job patches without messing with the MRO. Extends the main definitions of extract, transform, load,
        cache, and uncache. Adds all non-double underscore attributes and methods from injected classes.

        In short, inheritance without MRO changes, extra bases, derived class et c.
        :param args: Patches to be applied
        :return: The decorated class with attributes added/extended
        """
        def class_wrap(cls):
            def new_by_type(f_type):
                orig = getattr(cls, f_type)
                def new_func(self, **kwargs):
                    orig(self, **kwargs)
                    for inj in args:
                        if hasattr(inj, 'static_' + f_type):
                            getattr(inj, 'static_' + f_type)(self, **kwargs)
                    return self
                return new_func

            d = cls.__dict__.copy()
            d.update({
                m: new_by_type(m)
                for m in _job_methods
            })

            # build new init
            orig_init = cls.__init__
            def new_init(self, *init_args, **kwargs):
                orig_init(self, *init_args, **kwargs)

                # create patch jobs and append class methods and attributes to base job
                for a in args:
                    try:
                        next_patch = a(**kwargs)
                    except:
                        next_patch = a()

                    for a_name in dir(next_patch):
                        if a_name not in _job_methods and 'static_' not in a_name and not a_name.startswith('__'):
                            setattr(cls, a_name, getattr(next_patch, a_name))

            # make sure __init__ and ETL-CU are appended
            cls.__init__ = new_init
            for attr in _job_methods:
                setattr(cls, attr, d[attr])
            return cls

        return class_wrap

    @staticmethod
    def extractors(**kwargs):
        """
        Add extractors to a job. These are functions that do not take self.
        :param kwargs: name_of_attribute_to_store_data_in = extractor_function
        :return: wrapped class with the appended extractors
        """
        def class_wrap(cls):
            orig_f = getattr(cls, 'extract')
            def new_function(self, **nf_kwargs):
                # call parent extract
                orig_f(self, **nf_kwargs)
                for k, v in kwargs.items():
                    setattr(self, k, v(**nf_kwargs))
                return self

            return type(cls.__name__, (cls,), { 'extract': new_function }
            )
        return class_wrap

    @staticmethod
    def transformers(*args):
        """
        Add basic transformers to a job. These are functions that do not take self.
        :param args: function w signature f(data_to_be_transformed, **kwargs) that returns post transform data
        :return: wrapped class with the appended transformers
        """
        def class_wrap(cls):
            orig_f = getattr(cls, 'transform')
            def new_function(self, **nf_kwargs):
                # call parent transform
                orig_f(self, **nf_kwargs)

                # if original transformer did anything get the transformed_data
                next_data = getattr(self, 'transformed_data')
                if next_data is None:
                    # otherwise the first *args transformer should start with extracted_data
                    next_data = getattr(self, 'extracted_data')

                for a in args:
                    setattr(self, 'transformed_data', a(next_data, **nf_kwargs))
                    next_data = getattr(self, 'transformed_data')
                return self

            return type(cls.__name__, (cls,), { 'transform': new_function })
        return class_wrap

    @staticmethod
    def create(job_name, extract=None, transform=None, load=None, cache=None, uncache=None, **kwargs):
        def as_job_m(m, attr, prior_attr=None):
            if m is not None:
                def wrapped(self, **kwargs):
                    if prior_attr is not None:
                        kwargs[prior_attr] = getattr(self, prior_attr)
                    res = m(**kwargs)
                    if attr is not None:
                        setattr(self, attr, res)
                    return self
                return wrapped
            else:
                return lambda self, **kwargs: self

        new_job_type = type(job_name, (Job,), {
            'extract': as_job_m(extract, 'extracted_data'),
            'transform': as_job_m(transform, 'transformed_data', 'extracted_data'),
            'load': as_job_m(load, None, 'transformed_data'),
            'cache': as_job_m(cache, None, 'transformed_data'),
            'uncache': as_job_m(uncache, None, 'transformed_data')
        })

        return Job.dependency(**kwargs)(new_job_type) if kwargs else new_job_type

    def __init__(self):
        self.extracted_data = None
        self.transformed_data = None

    def extract(self, **kwargs):
        return self

    def transform(self, **kwargs):
        return self

    def load(self, **kwargs):
        return self

    def cache(self, **kwargs):
        return self

    def uncache(self, **kwargs):
        return self


class ParentJobException(Exception):
     def __init__(self, parent_job_node, **kwargs):
         super(ParentJobException, self).__init__(**kwargs)
         self.parent_job_node = parent_job_node


class JobNode(TreeNode):
    def __init__(self, job):
        assert isinstance(job, Job)
        super(JobNode, self).__init__(job.__class__.__name__, job)
        self.status = JOB_STATUS.QUEUE
        self.error = None


class JobRunner(object):
    def __init__(self, jobs=None):
        self.__ptree = PolyTree()
        if jobs:
            self.add_jobs(jobs)

        self.status = JOB_STATUS.QUEUE

    def add_job(self, job):
        # create job node
        job_node = JobNode(job)
        if self.__ptree.node_exists(job_node):
            # explicitly added jobs >> implicit
            # result:
            #   if job added twice, second is kept.
            #   if job was added by parent inference, it's overwritten
            self.__ptree.get_node(job_node.id).data = job

        # get parents
        parents = [
            JobNode(parent())
            for parent in job.ETL_SIGNATURE.values()
        ] if hasattr(job, 'ETL_SIGNATURE') else []

        # add to job poly tree
        self.__ptree.add_node(job_node, parents)

        return self

    def add_jobs(self, jobs):
        [ self.add_job(j) for j in jobs ]
        return self

    def __get_job_kwargs(self, job):
        if hasattr(job, 'ETL_SIGNATURE'):
            return {
                param: self.__ptree.get_node(type_source.__name__).data.transformed_data
                for param, type_source in job.ETL_SIGNATURE.items()
            }
        else:
            return {}

    # runs a job and caches if needed
    def __run_single_job(self, job_node):
        job_node.status = JOB_STATUS.RUNNING
        try:
            job_node.data.extract()

            # stage/run job
            job_node.data.transform(**self.__get_job_kwargs(job_node.data))

            # if there are queued up children jobs, cache results
            if len(self.children_in_queue(job_node.data)) > 0:
                job_node.data.cache()

            # load results
            job_node.data.load()
            job_node.status = JOB_STATUS.DONE
        except Exception as e:
            job_node.error = e
            job_node.status = JOB_STATUS.FAILED

    # runs a job and all its parents
    def __run_job_line(self, job_node):

        # run parent jobs
        for parent in self.__ptree.parents(job_node):
            # no need to walk the whole chain if immediate parent is already done
            check_status = self.__run_job_line(parent) if parent.status == JOB_STATUS.QUEUE else parent.status
            if check_status == JOB_STATUS.FAILED:
                job_node.status = JOB_STATUS.FAILED
                job_node.error = ParentJobException(parent)


        # run current job
        if job_node.status == JOB_STATUS.QUEUE:
            self.__run_single_job(job_node)

        # uncache parents that are no longer needed
        for parent in self.__ptree.parents(job_node):
            if len(self.children_in_queue(parent.data)) == 0:
                parent.data.uncache()

        return job_node.status

    def run(self, start_from=None):
        if start_from is not None:
            raise NotImplementedError()

        self.status = JOB_STATUS.RUNNING

        for jn in self.__ptree.end_nodes():
            self.__run_job_line(jn)

        self.status = JOB_STATUS.FAILED if len(self.failed_jobs()) else JOB_STATUS.DONE

        return self

    def children_in_queue(self, job):
        return [
            child_job_node.data
            for child_job_node in self.__ptree.children(JobNode(job))
            if child_job_node.status == JOB_STATUS.QUEUE
        ]

    def failed_jobs(self):
        return [ node.data for node in self.__ptree.nodes() if node.status == JOB_STATUS.FAILED ]

    def failed_job_roots(self):
        return [
            node.data
            for node in self.__ptree.nodes()
            if node.error is not None and not isinstance(node.error, ParentJobException)
        ]

    def failed_job_root_paths(self):
        return {
            fail_root: self.all_paths(fail_root)
            for fail_root in self.failed_job_roots()
        }

    def all_paths(self, job):
        return [
            [ path_item.data for path_item in path ]
            for path in self.__ptree.all_paths(JobNode(job))
        ]

    def jobs(self):
        return [ node.data for node in self.__ptree.nodes() ]

    def reset_jobs(self):
        for job_node in self.__ptree.nodes():
            job_node.status = JOB_STATUS.QUEUE

    def clear_jobs(self):
        return self.__ptree.clear_nodes()
